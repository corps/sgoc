package io.corps.sgoc.persistence.session.backend.mysql;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.corps.sgoc.persistence.session.LogStateManager;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.schema.tables.Tables;
import io.corps.sgoc.schema.tables.tables.records.ObjectsRecord;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.persistence.session.backend.Backend;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class MysqlBackend implements Backend, Closeable {
  private static final int DEFAULT_LOOKUP_PARTITION_SIZE = 100;
  public static final Function<Sync.ObjectId, String> UUID_OF_OBJECT_ID_F = new Function<Sync.ObjectId, String>() {
    @Override
    public String apply(Sync.ObjectId input) {
      return input.getUuid();
    }
  };

  private final DefaultConnectionProvider connectionProvider;
  private final DSLContext dslContext;
  private final Connection connection;
  private final int lookupPartitionSize;

  public MysqlBackend(Connection connection) {
    this(connection, new Settings(), DEFAULT_LOOKUP_PARTITION_SIZE);
  }

  public MysqlBackend(Connection connection, Settings settings) {
    this(connection, settings, DEFAULT_LOOKUP_PARTITION_SIZE);
  }

  public MysqlBackend(Connection connection, Settings settings, int lookupPartitionSize) {
    this.connection = connection;
    this.lookupPartitionSize = lookupPartitionSize;
    this.connectionProvider = new DefaultConnectionProvider(connection);
    this.connectionProvider.setAutoCommit(false);
    dslContext = DSL.using(connectionProvider, SQLDialect.MYSQL, settings);
  }

  @Override
  public void close() throws IOException {
    connectionProvider.rollback();
    try {
      connection.close();
    } catch (SQLException e) {
      throw new IOException("Failed to close the connection!", e);
    }
  }

  @Override
  public List<Sync.ObjectWrapper> lookup(String rootKey, List<Sync.ObjectId> idsToLookup) throws IOException {
    List<Sync.ObjectWrapper> result = Lists.newArrayListWithExpectedSize(idsToLookup.size());

    for (List<Sync.ObjectId> objectIds : Lists.partition(idsToLookup, lookupPartitionSize)) {
      SelectQuery<ObjectsRecord> query = dslContext.selectQuery(Tables.OBJECTS);

      query.addConditions(Tables.OBJECTS.ROOT_KEY.equal(rootKey));
      if (objectIds.size() == 1) {
        query.addConditions(Tables.OBJECTS.UUID.equal(objectIds.get(0).getUuid()));
      } else {
        query.addConditions(Tables.OBJECTS.UUID.in(Lists.transform(objectIds, UUID_OF_OBJECT_ID_F)));
      }

      Sync.ObjectId.Builder idBuilder = Sync.ObjectId.newBuilder();
      for (ObjectsRecord record : query.fetchLazy()) {
        idBuilder.setUuid(Preconditions.checkNotNull(record.getUuid()));
        byte[] objectBytes = Preconditions.checkNotNull(record.getObject());
        Sync.ObjectWrapper.Builder next = Sync.ObjectWrapper.newBuilder().mergeFrom(objectBytes);
        next.setId(idBuilder);
        result.add(next.build());
      }
    }

    return result;
  }

  // TODO: implement
  @Override
  public Multimap<IndexLookup, Sync.ObjectId> lookupIndexes(String rootKey, List<IndexLookup> lookups) throws IOException {
    return null;
  }

  @Override
  public long getLastTimestamp() throws IOException {
    return 0;
  }

  @Override
  public long getCurrentTimestamp() throws IOException {
    return 0;
  }

  @Override
  public long getLastLogSequence() throws IOException {
    return 0;
  }

  @Override
  public void writeLogEntry(LogStateManager.LogState logState) throws IOException {

  }

  @Override
  public void writeObjects(Collection<Sync.ObjectWrapper> values) throws IOException {

  }

  @Override
  public void removeIndexEntries(Collection<Map.Entry<IndexLookup, Sync.ObjectId>> removedIndexEntries) throws IOException {

  }

  @Override
  public void addIndexEntries(Collection<Map.Entry<IndexLookup, Sync.ObjectId>> entries) throws IOException {

  }

  @Override
  public void commit() throws IOException {

  }

  @Override
  public List<Sync.ObjectWrapper> changesSince(long timestamp) throws IOException {
    return null;
  }
}
