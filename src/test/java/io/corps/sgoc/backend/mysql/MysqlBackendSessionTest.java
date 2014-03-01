package io.corps.sgoc.backend.mysql;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.protobuf.ExtensionRegistry;
import io.corps.sgoc.schema.*;
import io.corps.sgoc.session.LogStateManager;
import io.corps.sgoc.session.SgocTestDatabase;
import io.corps.sgoc.session.exceptions.WriteContentionException;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.Fixtures;
import io.corps.sgoc.utils.MultimapUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.*;

import static io.corps.sgoc.test.model.Test.apple;
import static org.junit.Assert.fail;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class MysqlBackendSessionTest {
  Set<Sync.ObjectWrapper> objectGroupA;
  Set<Sync.ObjectWrapper> filteredA;
  Set<Sync.ObjectWrapper> objectGroupB;
  Set<Sync.ObjectWrapper> filteredB;
  private Connection connection;
  private MysqlBackendSession backend;
  private String rootKey1;
  private String rootKey2;
  private Predicate<Sync.ObjectWrapper> randomFilter = new Predicate<Sync.ObjectWrapper>() {
    @Override
    public boolean apply(Sync.ObjectWrapper input) {
      return new Random().nextBoolean();
    }
  };
  private LogStateManager logStateManager1;
  private LogStateManager logStateManager2;
  private EntitySchema schema;
  private PayloadEntity appleEntity;

  @After
  public void tearDown() throws Exception {
    connection.close();
  }

  @Before
  public void setUp() throws Exception {
    schema = new EntitySchema(io.corps.sgoc.test.model.Test.getDescriptor());
    appleEntity = schema.getEntity(apple.getDescriptor());

    rootKey1 = "someguy";
    rootKey2 = "otherguy";

    objectGroupA = Sets.newHashSet();
    objectGroupB = Sets.newHashSet();

    for (int i = 0; i < 50; ++i) {
      objectGroupA.add(Fixtures.wrapABasket());
      objectGroupA.add(Fixtures.wrapAnApple());
      objectGroupA.add(Fixtures.wrapAnOrange());
      objectGroupA.add(Fixtures.wrapASpaghetti());

      objectGroupB.add(Fixtures.wrapABasket());
      objectGroupB.add(Fixtures.wrapAnApple());
      objectGroupB.add(Fixtures.wrapAnOrange());
      objectGroupB.add(Fixtures.wrapASpaghetti());
    }

    filteredA = Sets.newHashSet(Sets.filter(objectGroupA, randomFilter));
    filteredB = Sets.newHashSet(Sets.filter(objectGroupB, randomFilter));

    connection = SgocTestDatabase.setup();
    ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
    Schema.registerAllExtensions(extensionRegistry);
    io.corps.sgoc.test.model.Test.registerAllExtensions(extensionRegistry);
    backend = new MysqlBackendSession(connection, extensionRegistry);

    logStateManager1 = new LogStateManager(rootKey1, backend);
    logStateManager2 = new LogStateManager(rootKey2, backend);
    logStateManager1.reset();
    logStateManager2.reset();
  }

  @Test
  public void testLogState() throws Exception {
    Assert.assertTrue(logStateManager1.getLogState().getLastTimestamp() > 0);
    Assert.assertEquals(0, logStateManager1.getLogState().getLastLogSequence());
    long previousWriteTimestamp = logStateManager1.getLogState().getWriteTimestamp();
    Assert.assertTrue(previousWriteTimestamp > logStateManager1.getLogState().getLastTimestamp());

    backend.writeLogEntry(rootKey1, logStateManager1.getLogState());

    logStateManager1.reset();
    Assert.assertEquals(1, logStateManager1.getLogState().getLastLogSequence());
    Assert.assertTrue(logStateManager1.getLogState().getWriteTimestamp() > previousWriteTimestamp);
    backend.writeLogEntry(rootKey1, logStateManager1.getLogState());

    try {
      backend.writeLogEntry(rootKey1, logStateManager1.getLogState());
      fail("Expected WriteContentionException");
    } catch (WriteContentionException e) {
    }

    backend.writeLogEntry(rootKey2, logStateManager2.getLogState());
  }

  @Test
  public void testLookupAndWriteObjects() throws Exception {
    backend.writeObjects(rootKey1, filteredA, logStateManager1.getLogState());
    backend.writeObjects(rootKey2, filteredB, logStateManager2.getLogState());

    Assert.assertEquals(filteredA, Sets.newHashSet(backend.lookup(rootKey1,
        Lists.newArrayList(Iterables.transform(objectGroupA, MysqlBackendSession.ID_OF_WRAPPER_F)))));

    Assert.assertTrue(backend.lookup(rootKey1,
        Lists.newArrayList(Iterables.transform(objectGroupB, MysqlBackendSession.ID_OF_WRAPPER_F))).isEmpty());

    Assert.assertEquals(filteredB, Sets.newHashSet(backend.lookup(rootKey2,
        Lists.newArrayList(Iterables.transform(objectGroupB, MysqlBackendSession.ID_OF_WRAPPER_F)))));

    Iterator<Sync.ObjectWrapper> iterator = filteredA.iterator();

    HashSet<Sync.ObjectWrapper> setAWithSomeB =
        Sets.newHashSet(Sets.union(filteredA, Sets.filter(filteredB, randomFilter)));

    for (int i = 0; i < 30; ++i) {
      Sync.ObjectWrapper toModify = iterator.next();
      setAWithSomeB.remove(toModify);
      setAWithSomeB.add(toModify.toBuilder().setVersion(new Random().nextInt(120)).build());
    }

    backend.writeObjects(rootKey1, setAWithSomeB, logStateManager1.getLogState());
    Assert.assertEquals(setAWithSomeB, Sets.newHashSet(backend.lookup(rootKey1, Lists.newArrayList(
        Iterables.transform(Sets.union(objectGroupA, objectGroupB), MysqlBackendSession.ID_OF_WRAPPER_F)))));
  }

  @Test
  public void testIndexes() throws Exception {
    SetMultimap<IndexLookup, String> indexes = MultimapUtils.createMultimap();

    Sync.ObjectWrapper objects[] = new Sync.ObjectWrapper[]{Fixtures.wrapAnApple(), Fixtures.wrapAnApple(),
        Fixtures.wrapAnApple(), Fixtures.wrapAnApple()};

    backend.writeObjects(rootKey1, Lists.newArrayList(objects), logStateManager1.getLogState());

    String ids[] = new String[]{objects[0].getId(), objects[1].getId(), objects[2].getId(),
        objects[3].getId()};

    Collection<EntityIndex> appleIndexes = appleEntity.getIndexes();
    appleIndexes.remove(appleEntity.getIndex("Ordinals"));
    appleIndexes.remove(appleEntity.getIndex("Basket Index"));

    EntityIndex ordinalIndex = appleEntity.getIndex("Ordinals");
    EntityIndex uniqueOrdinalIndex = appleIndexes.iterator().next();

    IndexLookup lookups[] = new IndexLookup[]{uniqueOrdinalIndex.lookup(new Object[]{2L, "asdf"}),
        uniqueOrdinalIndex.lookup(new Object[]{-102L, "asdf"}),
        uniqueOrdinalIndex.lookup(new Object[]{-102L, "cost"}),
        ordinalIndex.lookup(new Object[]{-102L}),
        ordinalIndex.lookup(new Object[]{14L})};

    indexes.put(lookups[0], ids[0]);
    indexes.put(lookups[0], ids[1]);
    indexes.put(lookups[1], ids[2]);
    indexes.put(lookups[2], ids[0]);
    indexes.put(lookups[3], ids[3]);
    indexes.put(lookups[4], ids[2]);
    indexes.put(lookups[4], ids[1]);

    backend.removeIndexEntries(rootKey1, indexes.entries(), logStateManager1.getLogState());
    Assert.assertEquals(MultimapUtils.createMultimap(), backend.lookupIndexes(rootKey1, Lists.newArrayList(lookups)));

    backend.addIndexEntries(rootKey1, indexes.entries(), logStateManager1.getLogState());

    Multimap<IndexLookup, String> expected = MultimapUtils.createMultimap();
    expected.putAll(lookups[0], indexes.get(lookups[0]));
    expected.putAll(lookups[1], indexes.get(lookups[1]));
    expected.putAll(lookups[2], indexes.get(lookups[2]));

    Multimap<IndexLookup, String> lookup = backend.lookupIndexes(rootKey1,
            Lists.newArrayList(lookups[0], lookups[1], lookups[2], ordinalIndex.lookup(new Object[]{1942L})));

    Assert.assertEquals(expected, lookup);

    Multimap<IndexLookup, String> removed = MultimapUtils.createMultimap();
    removed.putAll(lookups[0], Sets.newHashSet(ids[1]));
    removed.putAll(lookups[1], indexes.get(lookups[1]));

    expected.clear();
    expected.put(lookups[0], ids[0]);
    expected.putAll(lookups[2], indexes.get(lookups[2]));
    expected.putAll(lookups[3], indexes.get(lookups[3]));

    backend.removeIndexEntries(rootKey1, removed.entries(), logStateManager1.getLogState());

    Assert.assertEquals(expected, backend.lookupIndexes(rootKey1,
        Lists.newArrayList(lookups[0], lookups[1], lookups[2], lookups[3])));
    Assert.assertTrue(backend.lookupIndexes(rootKey2, indexes.keySet()).isEmpty());
  }

  @Test
  public void testChangesSince() throws Exception {
    Assert.assertTrue(backend.changesSince(rootKey1, -1).isEmpty());

    LogStateManager.LogState logState = logStateManager1.getLogState();
    backend.writeObjects(rootKey1, filteredA, logState);
    backend.writeLogEntry(rootKey1, logState);
    List<Sync.ObjectWrapper> changes = backend.changesSince(rootKey1, -1);
    Assert.assertEquals(Sets.newHashSet(changes), Sets.newHashSet(backend.changesSince(rootKey1, 0)));
    Assert.assertEquals(Sets.newHashSet(filteredA), Sets.newHashSet(changes));

    Assert.assertTrue(backend.changesSince(rootKey2, -1).isEmpty());
    HashSet<Sync.ObjectWrapper> doubleFilteredA = Sets.newHashSet(Sets.filter(filteredA, randomFilter));
    backend.writeObjects(rootKey2, doubleFilteredA, logStateManager2.getLogState());
    backend.writeLogEntry(rootKey2, logStateManager2.getLogState());

    long writeTimestamp = logState.getWriteTimestamp();
    Assert.assertTrue(backend.changesSince(rootKey1, writeTimestamp).isEmpty());

    logStateManager1.reset();
    logState = logStateManager1.getLogState();
    backend.writeObjects(rootKey1, doubleFilteredA, logState);
    backend.writeLogEntry(rootKey1, logState);
    Assert.assertEquals(writeTimestamp, logState.getLastTimestamp());
    Assert.assertEquals(Sets.newHashSet(doubleFilteredA), Sets.newHashSet(
        backend.changesSince(rootKey1, writeTimestamp)));

    HashSet<Sync.ObjectWrapper> deletedObjects = Sets.newHashSet(Iterables
        .transform(Sets.filter(doubleFilteredA, randomFilter), new Function<Sync.ObjectWrapper, Sync.ObjectWrapper>() {
          @Override
          public Sync.ObjectWrapper apply(Sync.ObjectWrapper input) {
            return input.toBuilder().setDeleted(true).build();
          }
        }));
    // New, immediately deleted object.
    deletedObjects.add(Fixtures.wrapABasket().toBuilder().setDeleted(true).build());

    logStateManager1.reset();
    logState = logStateManager1.getLogState();
    backend.writeObjects(rootKey1, deletedObjects, logState);

    logStateManager1.reset();
    logState = logStateManager1.getLogState();
    Assert.assertEquals(Sets.newHashSet(deletedObjects),
        Sets.newHashSet(backend.changesSince(rootKey1, logState.getLastTimestamp())));

    List<Sync.ObjectWrapper> objects = backend.changesSince(rootKey1, 0);
    for (Sync.ObjectWrapper object : filteredA) {
      Sync.ObjectWrapper deletedObject = object.toBuilder().setDeleted(true).build();

      if (deletedObjects.contains(deletedObject)) {
        Assert.assertFalse(objects.contains(deletedObject));
      } else {
        Assert.assertTrue(objects.contains(object));
      }
    }

    objects = backend.changesSince(rootKey1, 0);
    for (Sync.ObjectWrapper object : filteredA) {
      Sync.ObjectWrapper deletedObject = object.toBuilder().setDeleted(true).build();
      if (deletedObjects.contains(deletedObject)) {
        Assert.assertFalse(objects.contains(deletedObject));
      } else {
        Assert.assertTrue(objects.contains(object));
      }
    }
  }
}
