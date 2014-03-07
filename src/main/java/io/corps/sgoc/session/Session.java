package io.corps.sgoc.session;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ExtensionRegistry;
import io.corps.sgoc.backend.BackendSession;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.session.config.SessionConfig;
import io.corps.sgoc.session.exceptions.DependentPutReorderingRequest;
import io.corps.sgoc.session.trigger.AfterPutTrigger;
import io.corps.sgoc.session.trigger.BeforePutTrigger;
import io.corps.sgoc.session.trigger.TransactionTrigger;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.utils.MultimapUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class Session implements ReadWriteSession {
  public static final int FRESH_SYNC_VERSION_NUMBER = 0;
  private final String rootKey;
  private final BackendSession backendSession;
  private final EntitySchema entitySchema;
  private final ExtensionRegistry extensionRegistry;
  private final Set<BeforePutTrigger> beforePutTriggers;
  private final Set<AfterPutTrigger> afterPutTriggers;
  private final Set<TransactionTrigger> transactionTriggers;
  private final Set<IndexLookup> loadedIndexLookups = new HashSet<>();
  private final Set<String> loadedObjects = new HashSet<>();
  private final Map<String, Sync.ObjectWrapper> mutatedObjects = new HashMap<>();
  private final Map<String, Sync.ObjectWrapper> objectsInPersistence = new HashMap<>();
  private final SetMultimap<IndexLookup, String> existingIndexEntries = MultimapUtils.createMultimap();
  private final SetMultimap<IndexLookup, String> addedIndexEntries = MultimapUtils.createMultimap();
  private final SetMultimap<IndexLookup, String> removedIndexEntries = MultimapUtils.createMultimap();
  private final LogStateManager logStateManager;
  private boolean closed = false;
  private boolean successfulSave = false;
  private boolean opened = false;

  public Session(String rootKey, BackendSession backendSession, SessionConfig config) {
    this.rootKey = rootKey;
    this.backendSession = backendSession;

    this.extensionRegistry = Preconditions.checkNotNull(config.getExtensionRegistry());

    this.beforePutTriggers = Preconditions.checkNotNull(config.getBeforePutTriggers());
    this.afterPutTriggers = Preconditions.checkNotNull(config.getAfterPutTriggers());
    this.transactionTriggers = Preconditions.checkNotNull(config.getTransactionTriggers());

    this.entitySchema = Preconditions.checkNotNull(config.getEntitySchema());
    this.logStateManager = new LogStateManager(rootKey, backendSession);
  }

  @Override
  public void prefetchObjects(Collection<String> objectIds) throws IOException {
    ensureOpen();
    ensureObjectIdsValid(objectIds);

    // Actual ids to lookup, a subset of the given objectIds
    List<String> idsToLookup = Lists.newArrayListWithExpectedSize(objectIds.size());
    for (String objectId : objectIds) {
      if (!loadedObjects.contains(objectId)) {
        loadedObjects.add(objectId);
        idsToLookup.add(objectId);
      }
    }

    if (idsToLookup.isEmpty()) {
      return;
    }

    for (Sync.ObjectWrapper object : backendSession.lookup(rootKey, idsToLookup)) {
      objectsInPersistence.put(object.getId(), object);
    }
  }

  @Override
  public void prefetchIndexLookups(Iterable<IndexLookup> indexLookups) throws IOException {
    ensureOpen();

    List<IndexLookup> neededLookups =
        Lists.newArrayList(Iterables.filter(indexLookups, new Predicate<IndexLookup>() {
          @Override
          public boolean apply(IndexLookup input) {
            return !loadedIndexLookups.contains(input);
          }
        }));

    loadedIndexLookups.addAll(neededLookups);
    existingIndexEntries.putAll(backendSession.lookupIndexes(rootKey, neededLookups));
  }

  @Override
  public void open() throws IOException {
    if (opened || closed) {
      throw new IOException("Opened Session that was already opened!");
    }
    logStateManager.reset();
    opened = true;
  }

  @Override
  public Sync.ObjectWrapper get(String id) throws IOException {
    Sync.ObjectWrapper object = mutatedObjects.get(id);
    if (object == null) {
      prefetchObjects(Lists.newArrayList(id));
      object = objectsInPersistence.get(id);
    }
    return object;
  }

  @Override
  public Set<String> indexLookup(IndexLookup indexLookup) throws IOException {
    if (!loadedIndexLookups.contains(indexLookup)) {
      prefetchIndexLookups(Lists.newArrayList(indexLookup));
    }

    return Sets.union(
        Sets.difference(existingIndexEntries.get(indexLookup), removedIndexEntries.get(indexLookup)),
        addedIndexEntries.get(indexLookup));
  }

  @Override
  public Sync.GetResponse changesSince(long timestamp) throws IOException {
    ensureOpen();

    List<Sync.ObjectWrapper> changedObjects = backendSession.changesSince(rootKey, timestamp);
    Sync.GetResponse.Builder response = Sync.GetResponse.newBuilder();

    response.setCurrentVersion(logStateManager.getLogState().getLastTimestamp());
    response.addAllChangedObject(changedObjects);

    return response.build();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      if(!successfulSave) {
        backendSession.rollback();
      }
    }
    closed = true;
  }

  @Override
  public void put(Sync.ObjectWrapper objectWrapper) throws IOException {
    ensureOpen();

    put(objectWrapper, Collections.<String>emptySet());
  }

  @Override
    public void put(Sync.ObjectWrapper objectWrapper, Set<String> objectIds) throws  IOException,
      DependentPutReorderingRequest{
    ensureOpen();

    objectWrapper = Preconditions.checkNotNull(objectWrapper);
    ensureObjectWrapperIsValid(objectWrapper);
    String id = objectWrapper.getId();

    PayloadEntity payloadEntity = entitySchema.getEntity(objectWrapper);
    Sync.ObjectWrapper existingObject = get(id);
    objectWrapper = performBeforePutTriggers(objectWrapper, existingObject, objectIds);

    mutatedObjects.put(id, objectWrapper);
    updateIndexMutationState(payloadEntity.getIndexes(), objectWrapper, existingObject);

    performAfterPutTriggers(payloadEntity, existingObject, objectWrapper);
  }

  @Override
  public void reject(Sync.ObjectWrapper objectWrapper) throws IOException {
    ensureOpen();

    Sync.ObjectWrapper existing = get(objectWrapper.getId());

    if (existing == null) {
      put(objectWrapper.toBuilder().setDeleted(true).build());
    } else {
      put(existing);
    }
  }

  @Override
  public void save() throws IOException {
    ensureOpen();

    try {
      writeLogEntry();
      applyChanges();
      finalizeChanges();
      successfulSave = true;
    } finally {
      close();
      performAfterCommitTriggers(successfulSave);
    }

  }

  private void performAfterPutTriggers(PayloadEntity entityDescriptor,
                                       Sync.ObjectWrapper existingObject, Sync.ObjectWrapper objectWrapper)
      throws IOException {
    for (AfterPutTrigger trigger : afterPutTriggers) {
      trigger.afterPut(this, entitySchema, objectWrapper, existingObject);
    }
  }

  private void updateIndexMutationState(Collection<EntityIndex> entityIndexes,
                                        final Sync.ObjectWrapper objectWrapper,
                                        Sync.ObjectWrapper existingObject) throws IOException {
    String id = objectWrapper.getId();

    if (existingObject != null && !existingObject.getDeleted()) {
      Iterable<IndexLookup> oldIndexEntries =
          Iterables.transform(entityIndexes, new Function<EntityIndex, IndexLookup>() {
            @Override
            public IndexLookup apply(EntityIndex input) {
              return input.lookup(objectWrapper);
            }
          });

      prefetchIndexLookups(oldIndexEntries);
    }

    for (EntityIndex index : entityIndexes) {
      if (existingObject != null && !existingObject.getDeleted()) {
        IndexLookup existingLookup = index.lookup(existingObject);
        addedIndexEntries.remove(existingLookup, id);
        removedIndexEntries.put(existingLookup, id);
      }

      IndexLookup newLookup = index.lookup(objectWrapper);
      removedIndexEntries.remove(newLookup, id);

      if (!objectWrapper.getDeleted()) {
        if (!existingIndexEntries.containsEntry(newLookup, id)) {
          addedIndexEntries.put(newLookup, id);
        }
      }
    }
  }

  private Sync.ObjectWrapper performBeforePutTriggers(Sync.ObjectWrapper objectWrapper,
                                                      Sync.ObjectWrapper existingObject,
                                                      Set<String> objectIds) throws IOException {
    for (BeforePutTrigger trigger : beforePutTriggers) {
      objectWrapper = Preconditions.checkNotNull(trigger.beforePut(this, entitySchema, objectWrapper, existingObject,
          objectIds));
    }

    return objectWrapper;
  }

  private void finalizeChanges() throws IOException {
    backendSession.commit();
  }

  private void applyChanges() throws IOException {
    LogStateManager.LogState logState = logStateManager.getLogState();
    backendSession.writeObjects(rootKey, mutatedObjects.values(), logState);
    backendSession.removeIndexEntries(rootKey, removedIndexEntries.entries(), logState);
    backendSession.addIndexEntries(rootKey, addedIndexEntries.entries(), logState);
  }

  private void writeLogEntry() throws IOException {
    backendSession.writeLogEntry(rootKey, logStateManager.getLogState());
  }

  private void performAfterCommitTriggers(boolean success) throws IOException {
    for (TransactionTrigger trigger : transactionTriggers) {
      if (success) {
        trigger.afterCommit(this, entitySchema, mutatedObjects.values());
      } else {
        trigger.afterRollback(this, entitySchema, mutatedObjects.values());
      }
    }
  }

  private void ensureObjectWrapperIsValid(Sync.ObjectWrapper objectWrapper) {
    ensureObjectIdsValid(Lists.newArrayList(objectWrapper.getId()));
    Preconditions.checkArgument(entitySchema.getPayloadDescriptorField(objectWrapper) != null);
  }

  private void ensureObjectIdsValid(Collection<String> objectIds) {
    for (String objectId : objectIds) {
      Preconditions.checkState(!objectId.isEmpty());
    }
  }

  private void ensureOpen() throws IOException {
    if (closed || !opened) {
      throw new IOException("Session is not open.");
    }
  }
}
