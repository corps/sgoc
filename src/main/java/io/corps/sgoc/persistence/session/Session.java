package io.corps.sgoc.persistence.session;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.corps.sgoc.persistence.session.backend.Backend;
import io.corps.sgoc.persistence.session.trigger.*;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.utils.MultimapUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
// In the future, would like to create separate implementations for ReadSession and WriteSession that needed
// only subsets of the backend, etc.
public class Session implements ReadWriteSession {
  public static final int FRESH_SYNC_VERSION_NUMBER = 0;
  private final String rootKey;
  private final Backend backend;
  private final EntitySchema entitySchema;
  private final Set<BeforePutTrigger> beforePutTriggers;
  private final Set<AfterPutTrigger> afterPutTriggers;
  private final Set<TransactionTrigger> transactionTriggers;
  private final Set<IndexLookup> loadedIndexLookups = new HashSet<>();
  private final Set<Sync.ObjectId> loadedObjects = new HashSet<>();
  private final Map<Sync.ObjectId, Sync.ObjectWrapper> mutatedObjects = new HashMap<>();
  private final Map<Sync.ObjectId, Sync.ObjectWrapper> objectsInPersistence = new HashMap<>();
  private final SetMultimap<IndexLookup, Sync.ObjectId> existingIndexEntries = MultimapUtils.createMultimap();
  private final SetMultimap<IndexLookup, Sync.ObjectId> addedIndexEntries = MultimapUtils.createMultimap();
  private final SetMultimap<IndexLookup, Sync.ObjectId> removedIndexEntries = MultimapUtils.createMultimap();
  private final LogStateManager logStateManager;
  private boolean closed = false;
  private boolean opened = false;

  // TODO:  Configuration Builder?
  public Session(String rootKey, Backend backend, EntitySchema entitySchema) {
    this(rootKey, backend, entitySchema, Collections.<BeforePutTrigger>emptySet(),
        Collections.<AfterPutTrigger>emptySet(), Collections.<TransactionTrigger>emptySet());
  }

  public Session(String rootKey, Backend backend, EntitySchema entitySchema,
                 Set<BeforePutTrigger> beforePutTriggers,
                 Set<AfterPutTrigger> afterPutTriggers,
                 Set<TransactionTrigger> transactionTriggers) {

    this.beforePutTriggers = Sets.newTreeSet(BeforePutTrigger.BY_PRIORITY);
    this.afterPutTriggers = Sets.newTreeSet(AfterPutTrigger.BY_PRIORITY);
    this.transactionTriggers = Sets.newTreeSet(TransactionTrigger.BY_PRIORITY);

    this.beforePutTriggers.addAll(beforePutTriggers);
    this.afterPutTriggers.addAll(afterPutTriggers);
    this.transactionTriggers.addAll(transactionTriggers);

    this.rootKey = rootKey;
    this.backend = backend;
    this.entitySchema = entitySchema;
    this.logStateManager = new LogStateManager(backend);
    addDefaultTriggers();
  }

  @Override
  public void prefetchObjects(Collection<Sync.ObjectId> objectIds) throws IOException {
    ensureOpen();
    ensureObjectIdsValid(objectIds);

    // Actual ids to lookup, a subset of the given objectIds
    List<Sync.ObjectId> idsToLookup = Lists.newArrayListWithExpectedSize(objectIds.size());
    for (Sync.ObjectId objectId : objectIds) {
      if (!loadedObjects.contains(objectId)) {
        loadedObjects.add(objectId);
        idsToLookup.add(objectId);
      }
    }

    if (idsToLookup.isEmpty()) {
      return;
    }

    for (Sync.ObjectWrapper object : backend.lookup(rootKey, idsToLookup)) {
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
    existingIndexEntries.putAll(backend.lookupIndexes(rootKey, neededLookups));
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
  public Sync.ObjectWrapper get(Sync.ObjectId id) throws IOException {
    Sync.ObjectWrapper object = mutatedObjects.get(id);
    if (object == null) {
      prefetchObjects(Lists.newArrayList(id));
      object = objectsInPersistence.get(id);
    }
    return object;
  }

  @Override
  public Set<Sync.ObjectId> indexLookup(IndexLookup indexLookup) throws IOException {
    if (!loadedIndexLookups.contains(indexLookup)) {
      prefetchIndexLookups(Lists.newArrayList(indexLookup));
    }

    return Sets.union(
        Sets.difference(existingIndexEntries.get(indexLookup), removedIndexEntries.get(indexLookup)),
        addedIndexEntries.get(indexLookup));
  }

  @Override
  public Sync.GetResponse changesSince(long timestamp) throws IOException {
    List<Sync.ObjectWrapper> changedObjects = backend.changesSince(timestamp);
    Sync.GetResponse.Builder response = Sync.GetResponse.newBuilder();

    response.setCurrentVersion(logStateManager.getLogState().getLastTimestamp());
    response.addAllChangedObject(changedObjects);

    return response.build();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      backend.close();
    }
    closed = true;
  }

  @Override
  public void put(Sync.ObjectWrapper objectWrapper) throws IOException {
    ensureOpen();

    objectWrapper = Preconditions.checkNotNull(objectWrapper);
    ensureObjectWrapperIsValid(objectWrapper);
    Sync.ObjectId id = objectWrapper.getId();

    PayloadEntity payloadEntity = entitySchema.getEntity(objectWrapper);
    Sync.ObjectWrapper existingObject = get(id);
    objectWrapper = performBeforePutTriggers(objectWrapper, existingObject);

    mutatedObjects.put(id, objectWrapper);
    updateIndexMutationState(payloadEntity.getIndexes(), objectWrapper, existingObject);

    performAfterPutTriggers(payloadEntity, existingObject, objectWrapper);
  }

  @Override
  public void reject(Sync.ObjectWrapper objectWrapper) throws IOException {
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

    boolean success = false;

    try {
      writeLogEntry();
      applyChanges();
      finalizeChanges();
      success = true;
    } finally {
      close();
      performAfterCommitTriggers(success);
    }

  }

  // TODO: Pull this shit out bro.
  private void addDefaultTriggers() {
    beforePutTriggers.add(new BadReferenceDeletePropagationTrigger());
    beforePutTriggers.add(new ClearDeletedPayloadsTrigger());
    beforePutTriggers.add(new IdempotentDeleteTrigger());
    beforePutTriggers.add(new ObjectVersioningTrigger());
    beforePutTriggers.add(new StripUnknownFieldsTrigger());
    beforePutTriggers.add(new RequiredValidationTrigger());
    afterPutTriggers.add(new OnDeletePropagationTrigger());
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
    Sync.ObjectId id = objectWrapper.getId();

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
                                                      Sync.ObjectWrapper existingObject) throws IOException {
    for (BeforePutTrigger trigger : beforePutTriggers) {
      objectWrapper = Preconditions.checkNotNull(trigger.beforePut(this, entitySchema, objectWrapper, existingObject));
    }

    return objectWrapper;
  }

  private void finalizeChanges() throws IOException {
    backend.commit();
  }

  private void applyChanges() throws IOException {
    backend.writeObjects(mutatedObjects.values());
    backend.removeIndexEntries(removedIndexEntries.entries());
    backend.addIndexEntries(addedIndexEntries.entries());
  }

  private void writeLogEntry() throws IOException {
    backend.writeLogEntry(logStateManager.getLogState());
  }

  private void performAfterCommitTriggers(boolean success) throws IOException {
    for (TransactionTrigger trigger : transactionTriggers) {
      if(success) {
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

  private void ensureObjectIdsValid(Collection<Sync.ObjectId> objectIds) {
    for (Sync.ObjectId objectId : objectIds) {
      Preconditions.checkState(!objectId.getUuid().isEmpty());
    }
  }

  private void ensureOpen() throws IOException {
    if (closed || !opened) {
      throw new IOException("Session is not open.");
    }
  }
}
