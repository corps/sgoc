package io.corps.sgoc.session;

import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.corps.sgoc.backend.BackendSession;
import io.corps.sgoc.schema.*;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.EqualWithoutRegardToOrder;
import io.corps.sgoc.testutils.Fixtures;
import io.corps.sgoc.testutils.TestSessionBuilder;
import io.corps.sgoc.utils.MultimapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class SessionTest {
  private BackendSession backendSession;
  private Session session;
  private String rootKey;
  private Sync.ObjectWrapper objectWrapper;
  private String id;
  private PayloadEntity entity;
  private EntitySchema schema;
  private long lastTimestamp;
  private EntityIndex basketOrdinalIndex;
  private EntityIndex ordinalIndex;
  private EntityIndex basketIndex;

  @Before
  public void setUp() throws Exception {
    rootKey = "theRootKey";
    backendSession = mock(BackendSession.class);
    lastTimestamp = 12L;
    when(backendSession.getLastTimestamp(rootKey)).thenReturn(lastTimestamp);
    schema = new EntitySchema(io.corps.sgoc.test.model.Test.getDescriptor());

    session = new Session(rootKey, backendSession, TestSessionBuilder.configBuilder().build());
    session.open();
    reset(backendSession);

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
    List<String> objectIds = new ArrayList<>();
    List<Sync.ObjectWrapper> objects = new ArrayList<>();

    objectIds.add(setupAnotherObject().getId());
    objects.add(setupAnotherObject());
    objectIds.add(objects.get(0).getId());
    objects.add(setupAnotherObject());
    objectIds.add(objects.get(1).getId());

    when(backendSession.lookup(rootKey, Lists.newArrayList(objectIds))).thenReturn(objects);
    session.prefetchObjects(objectIds);
    verify(backendSession).lookup(rootKey, objectIds);

    Assert.assertNull(session.get(objectIds.get(0)));
    Assert.assertEquals(objects.get(0), session.get(objectIds.get(1)));
    Assert.assertEquals(objects.get(1), session.get(objectIds.get(2)));

    verifyNoMoreInteractions(backendSession);
  }

  @Test
  public void testClose() throws Exception {
    session.close();
    verify(backendSession).rollback();
    reset(backendSession);

    // Closing multiple times is safe, but does not issue a downstream close to the backendSession but once.
    session.close();
    verifyZeroInteractions(backendSession);

    try {
      session.save();
      fail();
    } catch (IOException e) {
    }
  }

  @Test
  public void testSave() throws Exception {
    session.save();
    verify(backendSession).commit();
    verify(backendSession, never()).rollback();
    reset(backendSession);

    // A further close does not issue a second close to the backendSession.
    session.close();
    verifyZeroInteractions(backendSession);
  }

  @Test
  public void testPut() throws Exception {
    ArrayList<Sync.ObjectWrapper> resultingObjects = Lists.newArrayList();

    when(backendSession.lookup(rootKey, Lists.newArrayList(id))).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList());
    when(backendSession.lookupIndexes(anyString(), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, String>createMultimap()); // unique index lookup
    session.put(objectWrapper);
    resultingObjects.add(objectWrapper.toBuilder().setVersion(1).build());
    reset(backendSession);

    // No lookups are made -- the get should fetch the put value.
    Assert.assertEquals(objectWrapper.toBuilder().setVersion(1).build(), session.get(id));
    verifyNoMoreInteractions(backendSession);
    reset(backendSession);


    setupAnotherObject();
    resultingObjects.add(objectWrapper.toBuilder().setVersion(1).build());
    when(backendSession.lookup(rootKey, Lists.newArrayList(id))).thenReturn(
        Lists.<Sync.ObjectWrapper>newArrayList(objectWrapper.toBuilder().setVersion(-1).build()));
    when(backendSession.lookupIndexes(anyString(), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, String>createMultimap());
    session.put(objectWrapper);
    reset(backendSession);

    // No lookups are made -- the get should fetch the put value, and not the original persisted value.
    Assert.assertEquals(objectWrapper.toBuilder().setVersion(1).build(), session.get(id));
    verifyNoMoreInteractions(backendSession);
    reset(backendSession);

    when(backendSession.lookupIndexes(anyString(), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, String>createMultimap());

    setupAnotherObject();
    resultingObjects.add(objectWrapper.toBuilder().setVersion(1).build());
    session.put(objectWrapper.toBuilder().setVersion(-5).build());

    // Overrides previous put
    session.put(objectWrapper);

    session.save();
    verify(backendSession)
        .writeObjects(eq(rootKey), argThat(new EqualWithoutRegardToOrder<>(resultingObjects)), Matchers
            .<LogStateManager.LogState>any());
  }

  @Test
  public void testGet() throws Exception {
    ArrayList<String> idsToLookup = Lists.newArrayList(id);

    // First lookup defers to the backendSession to lookup the object.
    when(backendSession.lookup(rootKey, idsToLookup)).thenReturn(Lists.newArrayList(objectWrapper));
    Assert.assertEquals(objectWrapper, session.get(id));
    verify(backendSession).lookup(rootKey, idsToLookup);
    reset(backendSession);

    // Second lookup uses the cached value.
    when(backendSession.lookup(rootKey, idsToLookup)).thenReturn(Lists.newArrayList(objectWrapper));
    Assert.assertEquals(objectWrapper, session.get(id));
    verifyZeroInteractions(backendSession);
    reset(backendSession);
  }

  @Test
  public void testPutNewTombstoneObjectIndexSaves() throws Exception {
    when(backendSession.lookup(rootKey, Lists.newArrayList(id))).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList());
    objectWrapper = objectWrapper.toBuilder().setDeleted(true).build();
    when(backendSession.lookupIndexes(eq(rootKey), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, String>createMultimap());
    session.put(objectWrapper);

    reset(backendSession);
    session.save();

    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();
    SetMultimap<IndexLookup, String> removedIndexMultiMap = MultimapUtils.createMultimap();
    verify(backendSession)
        .addIndexEntries(eq(rootKey), eq(indexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
    verify(backendSession)
        .removeIndexEntries(eq(rootKey), eq(removedIndexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
  }

  @Test
  public void testPutNewObjectIndexSaves() throws Exception {
    when(backendSession.lookup(rootKey, Lists.newArrayList(id))).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList());
    when(backendSession.lookupIndexes(eq(rootKey), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, String>createMultimap());
    session.put(objectWrapper);

    reset(backendSession);
    session.save();

    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();
    SetMultimap<IndexLookup, String> removedIndexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(ordinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    verify(backendSession)
        .addIndexEntries(eq(rootKey), eq(indexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
    verify(backendSession)
        .removeIndexEntries(eq(rootKey), eq(removedIndexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
  }

  @Test
  public void testPutObjectOverTombstoneIndexSaves() throws Exception {
    // The existing object is already deleted.
    when(backendSession.lookup(rootKey, Lists.newArrayList(id)))
        .thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(objectWrapper.toBuilder().setVersion(-1).build()));
    when(backendSession.lookupIndexes(eq(rootKey), anyList()))
        .thenReturn(MultimapUtils.<IndexLookup, String>createMultimap());
    session.put(objectWrapper);

    reset(backendSession);
    session.save();

    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();
    SetMultimap<IndexLookup, String> removedIndexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(ordinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    verify(backendSession)
        .addIndexEntries(eq(rootKey), eq(indexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
    verify(backendSession)
        .removeIndexEntries(eq(rootKey), eq(removedIndexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
  }

  @Test
  public void testPutObjectOverExistingIndexSaves() throws Exception {
    // The existing object is already deleted.
    Sync.ObjectWrapper existingObject = Fixtures.wrapAnApple(objectWrapper,
        Fixtures.generateAnApple().setBasketId("Hi hi hi").setOrdinal(5).build());
    when(backendSession.lookup(rootKey, Lists.newArrayList(id)))
        .thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(existingObject));
    SetMultimap<IndexLookup, String> existingIndexes = MultimapUtils.createMultimap();
    existingIndexes.put(basketOrdinalIndex.lookup(existingObject), existingObject.getId());
    when(backendSession.lookupIndexes(eq(rootKey), anyList())).thenReturn(existingIndexes);
    session.put(objectWrapper);

    reset(backendSession);
    session.save();

    existingIndexes.put(ordinalIndex.lookup(existingObject), id);
    existingIndexes.put(basketIndex.lookup(existingObject), id);
    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(ordinalIndex.lookup(objectWrapper), id);
    verify(backendSession)
        .addIndexEntries(eq(rootKey), eq(indexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
    verify(backendSession)
        .removeIndexEntries(eq(rootKey), eq(existingIndexes.entries()), Matchers.<LogStateManager.LogState>any());
  }

  @Test
  public void testPutObjectOverExistingWithRedundentIndexSaves() throws Exception {
    // The existing object is already deleted.
    Sync.ObjectWrapper existingObject = Fixtures.wrapAnApple(objectWrapper,
        objectWrapper.getExtension(io.corps.sgoc.test.model.Test.apple).toBuilder().setBasketId("Hi hi hi").build());
    when(backendSession.lookup(rootKey, Lists.newArrayList(id)))
        .thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(existingObject));
    SetMultimap<IndexLookup, String> existingIndexes = MultimapUtils.createMultimap();
    existingIndexes.put(ordinalIndex.lookup(existingObject), existingObject.getId());
    when(backendSession.lookupIndexes(eq(rootKey), anyList())).thenReturn(existingIndexes);
    session.put(objectWrapper);

    reset(backendSession);
    session.save();

    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();
    indexMultiMap.put(basketOrdinalIndex.lookup(objectWrapper), id);
    indexMultiMap.put(basketIndex.lookup(objectWrapper), id);
    SetMultimap<IndexLookup, String> removedIndexes = MultimapUtils.createMultimap();
    removedIndexes.put(basketOrdinalIndex.lookup(existingObject), id);
    removedIndexes.put(basketIndex.lookup(existingObject), id);
    verify(backendSession)
        .addIndexEntries(eq(rootKey), eq(indexMultiMap.entries()), Matchers.<LogStateManager.LogState>any());
    verify(backendSession)
        .removeIndexEntries(eq(rootKey), eq(removedIndexes.entries()), Matchers.<LogStateManager.LogState>any());
  }

  @Test
  public void testIndexLookup() throws Exception {
    ArrayList<Sync.ObjectWrapper> objects =
        Lists.<Sync.ObjectWrapper>newArrayList(setupAnotherObject(5), setupAnotherObject(5));

    IndexLookup indexLookup = ordinalIndex.lookup(objectWrapper);
    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();

    indexMultiMap.put(ordinalIndex.lookup(objects.get(0)), objects.get(0).getId());
    indexMultiMap.put(ordinalIndex.lookup(objects.get(1)), objects.get(1).getId());
    when(backendSession.lookupIndexes(eq(rootKey), anyList())).thenReturn(indexMultiMap);
    when(backendSession.lookup(eq(rootKey), anyList())).thenReturn(Lists.<Sync.ObjectWrapper>newArrayList(objects));

    Assert.assertEquals(Sets.newHashSet(objects.get(0).getId(), objects.get(1).getId()),
        session.indexLookup(indexLookup));
    Assert.assertEquals(Sets.newHashSet(objects.get(0).getId(), objects.get(1).getId()),
        session.indexLookup(indexLookup));
    verify(backendSession, times(1)).lookupIndexes(eq(rootKey), anyList());

    session.put(objects.get(0).toBuilder().setDeleted(true).build());
    Assert.assertEquals(Sets.newHashSet(objects.get(1).getId()), session.indexLookup(indexLookup));

    setupAnotherObject(5);
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
    when(backendSession.changesSince(rootKey, 100)).thenReturn(Lists.newArrayList(objectWrapper));
    Assert.assertEquals(Lists.newArrayList(objectWrapper), session.changesSince(100).getChangedObjectList());
    Assert.assertEquals(lastTimestamp, session.changesSince(100).getCurrentVersion());
  }

  @Test
  public void testPrefetchIndexLookups() throws Exception {
    IndexLookup indexLookup = basketOrdinalIndex.lookup(objectWrapper);
    SetMultimap<IndexLookup, String> indexMultiMap = MultimapUtils.createMultimap();
    when(backendSession.lookupIndexes(rootKey, Lists.newArrayList(indexLookup))).thenReturn(indexMultiMap);
    session.prefetchIndexLookups(Lists.newArrayList(indexLookup));

    reset(backendSession);
    IndexLookup indexLookup2 = ordinalIndex.lookup(objectWrapper);
    when(backendSession.lookupIndexes(rootKey, Lists.newArrayList(indexLookup2))).thenReturn(indexMultiMap);
    session.prefetchIndexLookups(Lists.newArrayList(indexLookup, indexLookup2));
    verify(backendSession).lookupIndexes(rootKey, Lists.newArrayList(indexLookup2));
  }
  private Sync.ObjectWrapper setupAnotherObject() {
    return setupAnotherObject(new Random().nextInt(100000));
  }

  private Sync.ObjectWrapper setupAnotherObject(int ordinal) {
    id = Fixtures.generateUUID();
    objectWrapper = Fixtures.wrapAnApple(
        Sync.ObjectWrapper.newBuilder().setId(id)
            .build(), io.corps.sgoc.test.model.Test.Apple.newBuilder().setOrdinal(ordinal).build());
    id = objectWrapper.getId();
    return objectWrapper;
  }
}
