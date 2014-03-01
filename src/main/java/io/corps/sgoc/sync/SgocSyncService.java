package io.corps.sgoc.sync;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.corps.sgoc.backend.mysql.MysqlBackendSession;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.session.ReadWriteSession;
import io.corps.sgoc.session.exceptions.DependentPutReorderingRequest;
import io.corps.sgoc.session.exceptions.WriteContentionException;
import io.corps.sgoc.session.factory.ReadOnlySessionWork;
import io.corps.sgoc.session.factory.ReadWriteSessionWork;
import io.corps.sgoc.session.factory.SessionFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/03/03.
 * Copyrighted by Zach Collins 2014
 */
public class SgocSyncService implements SyncService {
  private static final int MAX_SANE_PUT_ATTEMPTS = 100;
  private final SessionFactory sessionFactory;
  private final EntitySchema entitySchema;

  public SgocSyncService(SessionFactory sessionFactory, EntitySchema entitySchema) {
    this.sessionFactory = sessionFactory;
    this.entitySchema = entitySchema;
  }

  @Override
  public Sync.GetResponse get(final Sync.GetRequest request) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(request.getRootId()));

    return sessionFactory.doReadOnlyWork(request.getRootId(),
        new ReadOnlySessionWork<Sync.GetResponse>() {
          @Override
          public Sync.GetResponse doWork(ReadSession session) throws IOException {
            return session.changesSince(request.getAppliedVersion());
          }
        });
  }

  @Override
  public Sync.PutResponse put(final Sync.PutRequest request) throws IOException {
    final Sync.PutResponse.Builder response = Sync.PutResponse.newBuilder();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(request.getRootId()));

    sessionFactory.doWork(request.getRootId(), new ReadWriteSessionWork<Object>() {
      @Override
      public Object doWork(ReadWriteSession session) throws IOException, WriteContentionException {
        session.prefetchObjects(Lists.transform(request.getObjectList(), MysqlBackendSession.ID_OF_WRAPPER_F));
        Set<IndexLookup> lookups = Sets.newHashSet();

        Map<String, Sync.ObjectWrapper> objects = Maps.newHashMap();
        for (Sync.ObjectWrapper objectWrapper : request.getObjectList()) {
          objects.put(objectWrapper.getId(), objectWrapper);
          PayloadEntity entity = entitySchema.getEntity(objectWrapper);
          for (EntityIndex entityIndex : entity.getIndexes()) {
            lookups.add(entityIndex.lookup(objectWrapper));
          }

          for (EntityIndex entityIndex : entitySchema.getReferencingIndexes(entity)) {
            String[] ids = new String[]{objectWrapper.getId()};
            lookups.add(entityIndex.lookup(ids));
          }
        }

        session.prefetchIndexLookups(lookups);

        LinkedList<String> idStack = Lists.newLinkedList();
        for (Sync.ObjectWrapper objectWrapper : request.getObjectList()) {
          idStack.push(objectWrapper.getId());
          while (!idStack.isEmpty()) {
            String id = idStack.pop();
            if (!objects.containsKey(id))
              continue; // already been processed!

            objectWrapper = Preconditions
                .checkNotNull(objects.get(id), "DependentPutReordering requested id not in current request!");

            try {
              session.put(objectWrapper, objects.keySet());
              objects.remove(objectWrapper.getId());
            } catch (DependentPutReorderingRequest reorderingRequest) {
              String dependentId = reorderingRequest.getObjectId();
              Preconditions.checkState(objects.containsKey(dependentId),
                  "DependentPutReordering for object that was already processed!");
              Preconditions
                  .checkState(!idStack.contains(objectWrapper.getId()), "DependentPutReordering cyclic reference!");

              idStack.push(objectWrapper.getId());
              idStack.push(dependentId);
            }
          }
        }

        session.save();
        return null;
      }
    });

    return response.build();
  }
}
