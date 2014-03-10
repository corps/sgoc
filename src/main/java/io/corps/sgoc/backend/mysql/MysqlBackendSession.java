package io.corps.sgoc.backend.mysql;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.SignedBytes;
import com.google.protobuf.ExtensionRegistry;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import io.corps.sgoc.backend.BackendSession;
import io.corps.sgoc.jooq.Tables;
import io.corps.sgoc.jooq.tables.records.IndexEntriesRecord;
import io.corps.sgoc.jooq.tables.records.LogEntriesRecord;
import io.corps.sgoc.jooq.tables.records.ObjectsRecord;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.schema.Schema;
import io.corps.sgoc.session.LogStateManager;
import io.corps.sgoc.session.Session;
import io.corps.sgoc.session.exceptions.WriteContentionException;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.utils.MultimapUtils;
import org.jooq.BatchBindStep;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultConnectionProvider;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class MysqlBackendSession implements BackendSession {
  public static final Function<Sync.ObjectWrapper, String> ID_OF_WRAPPER_F =
      new Function<Sync.ObjectWrapper, String>() {
        @Override
        public String apply(Sync.ObjectWrapper input) {
          return input.getId();
        }
      };
  private static final int DEFAULT_LOOKUP_PARTITION_SIZE = 100;
  private static final byte[] STUB_OBJECT = new byte[]{};
  private static final Long STUB_LONG = 0L;
  private static final String STUB_UUID = "?";
  private static final String STUB_ROOT_KEY = "??";
  private static final byte[] STUB_INDEX_VALUE = new byte[]{'a'};
  private static final byte[] STUB_INDEX_KEY = new byte[]{'b'};
  private static final int CHANGES_LAZY_BATCH_SIZE = 100;
  private static final Byte TRUE_BYTE = Byte.valueOf((byte) 1);
  private static final Byte FALSE_BYTE = Byte.valueOf((byte) 0);
  private static final Byte STUB_DELETED = TRUE_BYTE;
  private final DefaultConnectionProvider connectionProvider;
  private final DSLContext dslContext;
  private final Connection connection;
  private final int lookupPartitionSize;
  private final ExtensionRegistry extensionRegistry;

  public MysqlBackendSession(Connection connection, ExtensionRegistry extensionRegistry) {
    this(connection, new Settings().withExecuteLogging(true).withRenderFormatted(true), extensionRegistry);
  }

  public MysqlBackendSession(Connection connection, Settings settings, ExtensionRegistry extensionRegistry) {
    this(connection, settings, DEFAULT_LOOKUP_PARTITION_SIZE, extensionRegistry);
  }

  public MysqlBackendSession(Connection connection, Settings settings, int lookupPartitionSize,
                             ExtensionRegistry extensionRegistry) {
    this.connection = connection;
    this.lookupPartitionSize = lookupPartitionSize;
    this.connectionProvider = new DefaultConnectionProvider(connection);
    this.connectionProvider.setAutoCommit(false);
    this.extensionRegistry = extensionRegistry;
    Configuration configuration =
        new DefaultConfiguration().set(connectionProvider).set(SQLDialect.MYSQL).set(settings);

    dslContext = DSL.using(configuration);
  }

  @Override
  public void rollback() throws IOException {
    connectionProvider.rollback();
  }

  @Override
  public List<Sync.ObjectWrapper> lookup(String rootKey, Collection<String> idsToLookup) throws IOException {
    List<Sync.ObjectWrapper> result = Lists.newArrayListWithExpectedSize(idsToLookup.size());
    if (idsToLookup.isEmpty()) return result;

    for (List<String> objectIds : Iterables.partition(idsToLookup, lookupPartitionSize)) {
      SelectQuery<ObjectsRecord> query = dslContext.selectQuery(Tables.OBJECTS);

      query.addConditions(Tables.OBJECTS.ROOT_KEY.equal(rootKey));
      if (objectIds.size() == 1) {
        query.addConditions(Tables.OBJECTS.UUID.equal(objectIds.get(0)));
      } else {
        query.addConditions(Tables.OBJECTS.UUID.in(objectIds));
      }

      Sync.ObjectWrapper.Builder builder = Sync.ObjectWrapper.newBuilder();

      for (ObjectsRecord record : query.fetchLazy()) {
        byte[] objectBytes = Preconditions.checkNotNull(record.getObject());
        Sync.ObjectWrapper next = builder.mergeFrom(objectBytes, extensionRegistry).setId(record.getUuid()).build();
        result.add(next);
        builder.clear();
      }
    }

    return result;
  }

  @Override
  public Multimap<IndexLookup, String> lookupIndexes(String rootKey, Collection<IndexLookup> lookups)
      throws IOException {
    SetMultimap<IndexLookup, String> result = MultimapUtils.createMultimap();

    if (lookups.isEmpty()) {
      return result;
    }

    Iterable<List<IndexLookup>> partitions = Iterables.partition(lookups, 1000);
    for (List<IndexLookup> partition : partitions) {
      Select<IndexEntriesRecord> indexEntriesQuery = null;
      Map<Schema.IndexDescriptor, Map<byte[], IndexLookup>> lookupsByIndexDescriptor = Maps.newHashMap();

      for (IndexLookup indexLookup : partition) {
        Schema.IndexDescriptor indexDescriptor = indexLookup.getIndex().getIndexDescriptor();

        Map<byte[], IndexLookup> lookupsByIndexValue = lookupsByIndexDescriptor.get(indexDescriptor);
        if (lookupsByIndexValue == null) {
          lookupsByIndexValue = Maps.newTreeMap(SignedBytes.lexicographicalComparator());
          lookupsByIndexDescriptor.put(indexDescriptor, lookupsByIndexValue);
        }

        byte[] indexLookupValue = indexLookup.binaryEncodeValues();

        if(lookupsByIndexValue.containsKey(indexLookupValue))
          continue; // redunent query!

        lookupsByIndexValue.put(indexLookupValue, indexLookup);

        SelectQuery<IndexEntriesRecord> indexQuery = dslContext.selectQuery(Tables.INDEX_ENTRIES);
        indexQuery.addConditions(Tables.INDEX_ENTRIES.ROOT_KEY.equal(rootKey));
        indexQuery.addConditions(Tables.INDEX_ENTRIES.INDEX_KEY.equal(indexDescriptor.toByteArray()));
        indexQuery.addConditions(Tables.INDEX_ENTRIES.INDEX_VALUE.equal(indexLookupValue));

        if (indexEntriesQuery == null) {
          indexEntriesQuery = indexQuery;
        } else {
          indexEntriesQuery = indexEntriesQuery.unionAll(indexQuery);
        }
      }

      indexEntriesQuery = Preconditions.checkNotNull(indexEntriesQuery);
      for (IndexEntriesRecord indexEntry : indexEntriesQuery.fetchLazy()) {
        Map<byte[], IndexLookup> innerLookup =
            Preconditions.checkNotNull(lookupsByIndexDescriptor.get(
                Schema.IndexDescriptor.newBuilder().mergeFrom(indexEntry.getIndexKey(), extensionRegistry).build()));

        IndexLookup indexLookup = Preconditions.checkNotNull(innerLookup.get(indexEntry.getIndexValue()));

        result.put(indexLookup, indexEntry.getUuid());
      }
    }

    return result;
  }

  @Override
  public long getLastTimestamp(String rootKey) throws IOException {
    Record1<Long> timeStamp = dslContext.select(Tables.LOG_ENTRIES.TIMESTAMP)
        .from(Tables.LOG_ENTRIES)
        .where(Tables.LOG_ENTRIES.ROOT_KEY.equal(rootKey))
        .orderBy(Tables.LOG_ENTRIES.SEQUENCE.desc())
        .limit(1)
        .fetchAny();

    if (timeStamp == null) return System.currentTimeMillis();
    return timeStamp.value1();
  }

  @Override
  public long getCurrentTimestamp(String rootKey) throws IOException {
    return System.currentTimeMillis();
  }

  @Override
  public long getLastLogSequence(String rootKey) throws IOException {
    Record1<Long> logPos = dslContext.select(Tables.LOG_ENTRIES.SEQUENCE)
        .from(Tables.LOG_ENTRIES)
        .where(Tables.LOG_ENTRIES.ROOT_KEY.equal(rootKey))
        .orderBy(Tables.LOG_ENTRIES.SEQUENCE.desc())
        .limit(1)
        .fetchAny();

    if (logPos == null) return 0;
    return logPos.value1();
  }

  @Override
  public void writeLogEntry(String rootKey, LogStateManager.LogState logState) throws IOException {
    LogEntriesRecord record = new LogEntriesRecord();

    record.setRootKey(rootKey);
    record.setTimestamp(logState.getWriteTimestamp());
    record.setSequence(logState.getLastLogSequence() + 1);

    try {
      dslContext.executeInsert(record);
    } catch (DataAccessException e) {
      Throwable cause = e.getCause();
      // We will get a unique collision if we fail to write the log entry
      if (cause instanceof MySQLIntegrityConstraintViolationException) {
        throw new WriteContentionException(e);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void writeObjects(String rootKey, Collection<Sync.ObjectWrapper> values, LogStateManager.LogState logState)
      throws IOException {
    if (values.isEmpty()) return;

    for (List<Sync.ObjectWrapper> objectWrappers : Iterables.partition(values, lookupPartitionSize)) {
      BatchBindStep batch = dslContext.batch(dslContext.insertInto(Tables.OBJECTS,
          Tables.OBJECTS.OBJECT,
          Tables.OBJECTS.TIMESTAMP,
          Tables.OBJECTS.UUID,
          Tables.OBJECTS.ROOT_KEY,
          Tables.OBJECTS.DELETED).values(
          STUB_OBJECT,
          STUB_LONG,
          STUB_UUID,
          STUB_ROOT_KEY,
          STUB_DELETED
      ).onDuplicateKeyUpdate()
          .set(Tables.OBJECTS.OBJECT,
              DSL.function("VALUES", Tables.OBJECTS.OBJECT.getDataType(), Tables.OBJECTS.OBJECT))
          .set(Tables.OBJECTS.TIMESTAMP,
              DSL.function("VALUES", Tables.OBJECTS.TIMESTAMP.getDataType(), Tables.OBJECTS.TIMESTAMP))
          .set(Tables.OBJECTS.DELETED,
              DSL.function("VALUES", Tables.OBJECTS.DELETED.getDataType(), Tables.OBJECTS.DELETED))
      );

      for (Sync.ObjectWrapper objectWrapper : objectWrappers) {
        batch = batch.bind(objectWrapper.toByteArray(), logState.getWriteTimestamp(), objectWrapper.getId(),
            rootKey, objectWrapper.getDeleted() ? TRUE_BYTE : FALSE_BYTE);
      }

      batch.execute();
    }
  }

  @Override
  public void removeIndexEntries(String rootKey, Collection<Map.Entry<IndexLookup, String>> removedIndexEntries,
                                 LogStateManager.LogState logState)
      throws IOException {
    if (removedIndexEntries.isEmpty()) return;

    for (List<Map.Entry<IndexLookup, String>> partition :
        Iterables.partition(removedIndexEntries, lookupPartitionSize)) {
      BatchBindStep batch = dslContext.batch(dslContext.delete(Tables.INDEX_ENTRIES).where(
          Tables.INDEX_ENTRIES.ROOT_KEY.equal(STUB_ROOT_KEY)
              .and(Tables.INDEX_ENTRIES.INDEX_KEY.equal(STUB_INDEX_KEY))
              .and(Tables.INDEX_ENTRIES.INDEX_VALUE.equal(STUB_INDEX_VALUE))
              .and(Tables.INDEX_ENTRIES.UUID.equal(STUB_UUID))));

      batchIndexEntryProcess(rootKey, partition, batch);
    }
  }

  @Override
  public void addIndexEntries(String rootKey, Collection<Map.Entry<IndexLookup, String>> entries,
                              LogStateManager.LogState logState) throws IOException {
    if (entries.isEmpty()) return;

    for (List<Map.Entry<IndexLookup, String>> partition : Iterables.partition(entries, lookupPartitionSize)) {
      BatchBindStep batch = dslContext.batch(dslContext.insertInto(Tables.INDEX_ENTRIES,
          Tables.INDEX_ENTRIES.ROOT_KEY,
          Tables.INDEX_ENTRIES.INDEX_KEY,
          Tables.INDEX_ENTRIES.INDEX_VALUE,
          Tables.INDEX_ENTRIES.UUID)
          .values(STUB_ROOT_KEY, STUB_INDEX_KEY, STUB_INDEX_VALUE, STUB_UUID));

      batchIndexEntryProcess(rootKey, partition, batch);
    }
  }

  @Override
  public void commit() throws IOException {
    try {
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<Sync.ObjectWrapper> changesSince(String rootKey, long timestamp) throws IOException {
    ArrayList<Sync.ObjectWrapper> result = Lists.<Sync.ObjectWrapper>newArrayList();

    SelectQuery<ObjectsRecord> query = dslContext.selectQuery(Tables.OBJECTS);
    query.addConditions(Tables.OBJECTS.ROOT_KEY.equal(rootKey)
        .and(Tables.OBJECTS.TIMESTAMP.greaterThan(timestamp)));

    if (timestamp == Session.FRESH_SYNC_VERSION_NUMBER) {
      query.addConditions(Tables.OBJECTS.DELETED.isFalse());
    }

    Sync.ObjectWrapper.Builder objectBuilder = Sync.ObjectWrapper.newBuilder();
    for (ObjectsRecord record : query.fetchLazy(CHANGES_LAZY_BATCH_SIZE)) {
      objectBuilder.mergeFrom(record.getObject(), extensionRegistry);
      result.add(objectBuilder.build());
      objectBuilder.clear();
    }

    return result;
  }

  private void batchIndexEntryProcess(String rootKey, List<Map.Entry<IndexLookup, String>> partition,
                                      BatchBindStep batch) throws IOException {
    for (Map.Entry<IndexLookup, String> entry : partition) {
      IndexLookup lookup = entry.getKey();
      byte[] indexKey = lookup.binaryEncodeValues();
      String objectId = entry.getValue();

      batch = batch.bind(rootKey, lookup.getIndex().getIndexDescriptor().toByteArray(),
          indexKey, objectId);
    }

    batch.execute();
  }
}
