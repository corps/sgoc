package io.corps.sgoc.persistence.session;

import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.corps.sgoc.persistence.session.backend.Backend;
import io.corps.sgoc.schema.*;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.EqualWithoutRegardToOrder;
import io.corps.sgoc.testutils.Fixtures;
import io.corps.sgoc.utils.MultimapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class SessionTest {
  private Backend backend;
  private Session session;
  private String rootKey;
  private Sync.ObjectWrapper objectWrapper;
  private Sync.ObjectId id;
  private String uuid;
  private PayloadEntity entity;
  private EntitySchema schema;
  private long lastTimestamp;
  private EntityIndex basketOrdinalIndex;
  private EntityIndex ordinalIndex;
  private EntityIndex basketIndex;

  @Before
  public void setUp() throws Exception {
    rootKey = "theRootKey";
    backend = mock(Backend.class);
    lastTimestamp = 12L;
    when(backend.getLastTimestamp()).thenReturn(lastTimestamp);
    schema = new EntitySchema(io.corps.sgoc.test.model.Test.getDescriptor());
    session = new Session(rootKey, backend, schema);
    session.open();
    reset(backend);

    setupAnotherObject();


    entity = schema.getEntity(objectWrapper);

    basketOrdinalIndex = entity.getIndex(
        schema.getPayloadDescriptorField(objectWrapper).getMessageType().getOptions().getExtension(Schema.entity)
            .getIndexList().get(0));
    ordinalIndex = entity.getIndex("Ordinals");
    basketIndex = entity.getIndex("Basket Index");
  }

  @Test
  public void testPrefetchObjects() throws Exception {
    List<Sync.ObjectId> objectIds = new ArrayList<>();
    List<Sync.ObjectWrapper> objects = new ArrayList<>();

    objectIds.add(setupAnotherObject().getId());
    objects.add(setupAnotherObject());
    objectIds.add(objects.get(0).getId());
    objects.add(setupAnotherObject());
    objectIds.add(objects.get(1).getId());

    when(backend.lookup(rootKey, Lists.newArrayList(objectIds))).thenReturn(objects);
    session.prefetchObjects(objectIds);
    verify(backend).lookup(rootKey, objectIds);

    Assert.assertNull(session.get(objectIds.get(0)));
    Assert.assertEquals(objects.get(0), session.get(objectIds.get(1)));
    Assert.assertEquals(objects.get(1), session.get(objectIds.get(2)));

    verifyNoMoreInteractions(backend);
  }

  @Test
  public void testClose() throws Exception {
    session.close();
    verify(backend).close();
    reset(backend);

    // Closing multiple times is safe, but does not issue a downstream close to the backend but once.
    session.close();
    verifyZeroInteractions(backend);

    try {
      session.save();
      fail();
    } catch (IOException e) {
    }
  }

  @Test
  public void testSave() throws Exception {
    session.save();
    verify(backend).close();
    reset(backend);

    // A further close does not issue a second close to the backend.
    session.close();
    verifyZeroInteractions(backend);
  }

  @Test
  public void testPut() throws Exception {
    ArrayList<Sync.ObjectWrapper> resultingObjects = Lists.newArrayList();

    when(backend.lookup(rootKey, Lists.newArrayList(id))).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList());
    session.put(objectWrapper);
    resultingObjects.add(objectWrapper.toBuilder().setVersion(1).build());
    reset(backend);

    // No lookups are made -- the get should fetch the put value.
    Assert.assertEquals(objectWrapper.toBuilder().setVersion(1).build(), session.get(id));
    verifyNoMoreInteractions(backend);
    reset(backend);


    setupAnotherObject();
    resultingObjects.add(objectWrapper.toBuilder().setVersion(1).build());
    when(backend.lookup(rootKey, Lists.newArrayList(id))).thenReturn(
        Lists.<Sync.ObjectWrapper>newArrayList(objectWrapper.toBuilder().setVersion(-1).build()));
    when(backend.lookupIndexes(anyString(), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, Sync.ObjectId>createMultimap());
    session.put(objectWrapper);
    reset(backend);

    // No lookups are made -- the get should fetch the put value, and not the original persisted value.
    Assert.assertEquals(objectWrapper.toBuilder().setVersion(1).build(), session.get(id));
    verifyNoMoreInteractions(backend);
    reset(backend);

    when(backend.lookupIndexes(anyString(), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, Sync.ObjectId>createMultimap());

    setupAnotherObject();
    resultingObjects.add(objectWrapper.toBuilder().setVersion(1).build());
    session.put(objectWrapper.toBuilder().setVersion(-5).build());

    // Overrides previous put
    session.put(objectWrapper);

    session.save();
    verify(backend).writeObjects(argThat(new EqualWithoutRegardToOrder<>(resultingObjects)));
  }

  @Test
  public void testGet() throws Exception {
    ArrayList<Sync.ObjectId> idsToLookup = Lists.newArrayList(id);

    // First lookup defers to the backend to lookup the object.
    when(backend.lookup(rootKey, idsToLookup)).thenReturn(Lists.newArrayList(objectWrapper));
    Assert.assertEquals(objectWrapper, session.get(id));
    verify(backend).lookup(rootKey, idsToLookup);
    reset(backend);

    // Second lookup uses the cached value.
    when(backend.lookup(rootKey, idsToLookup)).thenReturn(Lists.newArrayList(objectWrapper));
    Assert.assertEquals(objectWrapper, session.get(id));
    verifyZeroInteractions(backend);
    reset(backend);
  }

  @Test
  public void testPutNewTombstoneObjectIndexSaves() throws Exception {
    when(backend.lookup(rootKey, Lists.newArrayList(id))).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList());
    objectWrapper = objectWrapper.toBuilder().setDeleted(true).build();
    when(backend.lookupIndexes(eq(rootKey), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, Sync.ObjectId>createMultimap());
    session.put(objectWrapper);

    reset(backend);
    session.save();

    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();
    SetMultimap<IndexLookup, Sync.ObjectId> removedIndexMultiMap = MultimapUtils.createMultimap();
    verify(backend).addIndexEntries(indexMultiMap.entries());
    verify(backend).removeIndexEntries(removedIndexMultiMap.entries());
  }

  @Test
  public void testPutNewObjectIndexSaves() throws Exception {
    when(backend.lookup(rootKey, Lists.newArrayList(id))).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList());
    when(backend.lookupIndexes(eq(rootKey), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, Sync.ObjectId>createMultimap());
    session.put(objectWrapper);

    reset(backend);
    session.save();

    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();
    SetMultimap<IndexLookup, Sync.ObjectId> removedIndexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(ordinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    verify(backend).addIndexEntries(indexMultiMap.entries());
    verify(backend).removeIndexEntries(removedIndexMultiMap.entries());
  }

  @Test
  public void testPutObjectOverTombstoneIndexSaves() throws Exception {
    // The existing object is already deleted.
    when(backend.lookup(rootKey, Lists.newArrayList(id)))
        .thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(objectWrapper.toBuilder().setVersion(-1).build()));
    when(backend.lookupIndexes(eq(rootKey), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, Sync.ObjectId>createMultimap());
    session.put(objectWrapper);

    reset(backend);
    session.save();

    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();
    SetMultimap<IndexLookup, Sync.ObjectId> removedIndexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(ordinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    verify(backend).addIndexEntries(indexMultiMap.entries());
    verify(backend).removeIndexEntries(removedIndexMultiMap.entries());
  }

  @Test
  public void testPutObjectOverExistingIndexSaves() throws Exception {
    // The existing object is already deleted.
    Sync.ObjectWrapper existingObject = Fixtures.wrapAnApple(objectWrapper,
        Fixtures.generateAnApple().setBasketId("Hi hi hi").setOrdinal(5).build());
    when(backend.lookup(rootKey, Lists.newArrayList(id)))
        .thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(existingObject));
    SetMultimap<IndexLookup, Sync.ObjectId> existingIndexes = MultimapUtils.createMultimap();
    existingIndexes.put(basketOrdinalIndex.lookup(existingObject), existingObject.getId());
    when(backend.lookupIndexes(eq(rootKey), anyList())).thenReturn(existingIndexes);
    session.put(objectWrapper);

    reset(backend);
    session.save();

    existingIndexes.put(ordinalIndex.lookup(existingObject), id);
    existingIndexes.put(basketIndex.lookup(existingObject), id);
    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(ordinalIndex.lookup(objectWrapper), id);
    verify(backend).addIndexEntries(indexMultiMap.entries());
    verify(backend).removeIndexEntries(existingIndexes.entries());
  }

  @Test
  public void testPutObjectOverExistingWithRedundentIndexSaves() throws Exception {
    // The existing object is already deleted.
    Sync.ObjectWrapper existingObject = Fixtures.wrapAnApple(objectWrapper,
        Fixtures.generateAnApple().setBasketId("Hi hi hi").build());
    when(backend.lookup(rootKey, Lists.newArrayList(id)))
        .thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(existingObject));
    SetMultimap<IndexLookup, Sync.ObjectId> existingIndexes = MultimapUtils.createMultimap();
    existingIndexes.put(ordinalIndex.lookup(existingObject), existingObject.getId());
    when(backend.lookupIndexes(eq(rootKey), anyList())).thenReturn(existingIndexes);
    session.put(objectWrapper);

    reset(backend);
    session.save();

    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    SetMultimap<IndexLookup, Sync.ObjectId> removedIndexes = MultimapUtils.createMultimap();
    removedIndexes.put(basketOrdinalIndex.lookup(existingObject), id);
    removedIndexes.put(basketIndex.lookup(existingObject), id);
    verify(backend).addIndexEntries(indexMultiMap.entries());
    verify(backend).removeIndexEntries(removedIndexes.entries());
  }

  @Test
  public void testIndexLookup() throws Exception {
    IndexLookup indexLookup = basketOrdinalIndex.lookup(objectWrapper);
    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();

    ArrayList<Sync.ObjectWrapper> objects =
        Lists.<Sync.ObjectWrapper>newArrayList(setupAnotherObject(), setupAnotherObject());

    indexMultiMap.put(basketOrdinalIndex.lookup(objects.get(0)), objects.get(0).getId());
    indexMultiMap.put(basketOrdinalIndex.lookup(objects.get(1)), objects.get(1).getId());
    when(backend.lookupIndexes(eq(rootKey), anyList())).thenReturn(indexMultiMap);
    when(backend.lookup(eq(rootKey), anyList())).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(objects));

    Assert.assertEquals(Sets.newHashSet(objects.get(0).getId(), objects.get(1).getId()),
        session.indexLookup(indexLookup));
    Assert.assertEquals(Sets.newHashSet(objects.get(0).getId(), objects.get(1).getId()),
        session.indexLookup(indexLookup));
    verify(backend, times(1)).lookupIndexes(eq(rootKey), anyList());

    session.put(objects.get(0).toBuilder().setDeleted(true).build());
    Assert.assertEquals(Sets.newHashSet(objects.get(1).getId()), session.indexLookup(indexLookup));

    setupAnotherObject();
    objects.add(objectWrapper);
    session.put(objectWrapper);
    Assert.assertEquals(Sets.newHashSet(id, objects.get(1).getId()), session.indexLookup(indexLookup));

    Sync.ObjectWrapper modifiedObjectWrapper =
        Fixtures.wrapAnApple(objectWrapper.toBuilder().setVersion(1).build(),
            io.corps.sgoc.test.model.Test.Apple.newBuilder().setOrdinal(123).build());
    session.put(modifiedObjectWrapper);
    Assert.assertEquals(Sets.newHashSet(objects.get(1).getId()), session.indexLookup(indexLookup));
    Assert.assertEquals(Sets.newHashSet(id),
        session.indexLookup(ordinalIndex.lookup(modifiedObjectWrapper)));
  }

  @Test
  public void testChangesSince() throws Exception {
    when(backend.changesSince(100)).thenReturn(Lists.newArrayList(objectWrapper));
    Assert.assertEquals(Lists.newArrayList(objectWrapper), session.changesSince(100).getChangedObjectList());
    Assert.assertEquals(lastTimestamp, session.changesSince(100).getCurrentVersion());
  }

  @Test
  public void testPrefetchIndexLookups() throws Exception {
    IndexLookup indexLookup = basketOrdinalIndex.lookup(objectWrapper);
    SetMultimap<IndexLookup, Sync.ObjectId> indexMultiMap = MultimapUtils.createMultimap();
    when(backend.lookupIndexes(rootKey, Lists.newArrayList(indexLookup))).thenReturn(indexMultiMap);
    session.prefetchIndexLookups(Lists.newArrayList(indexLookup));

    reset(backend);
    IndexLookup indexLookup2 = ordinalIndex.lookup(objectWrapper);
    when(backend.lookupIndexes(rootKey, Lists.newArrayList(indexLookup2))).thenReturn(indexMultiMap);
    session.prefetchIndexLookups(Lists.newArrayList(indexLookup, indexLookup2));
    verify(backend).lookupIndexes(rootKey, Lists.newArrayList(indexLookup2));
  }

  private Sync.ObjectWrapper setupAnotherObject() {
    uuid = Fixtures.generateUUID();
    objectWrapper = Fixtures.wrapAnApple(
        Sync.ObjectWrapper.newBuilder().setId(Sync.ObjectId.newBuilder().setUuid(uuid).build())
            .build());
    id = objectWrapper.getId();
    return objectWrapper;
  }
}
