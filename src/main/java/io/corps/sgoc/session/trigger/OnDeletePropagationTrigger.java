package io.corps.sgoc.session.trigger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.session.ReadWriteSession;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.utils.MultimapUtils;
import io.corps.sgoc.utils.Wrappers;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/23.
 * Copyrighted by Zach Collins 2014
 */
public class OnDeletePropagationTrigger extends AfterPutTrigger {
  public OnDeletePropagationTrigger() {
    // Aggressively process cascades to begin new processing loops sooner.
    super(-10000);
  }

  @Override
  public void afterPut(ReadWriteSession session, EntitySchema entitySchema, final Sync.ObjectWrapper put,
                       Sync.ObjectWrapper previous) throws IOException {
    if (put.getDeleted() && (previous == null || !previous.getDeleted())) {
      PayloadEntity entity = entitySchema.getEntity(put);

      Set<EntityIndex> referencingIndexes = entitySchema.getReferencingIndexes(entity);

      Iterable<IndexLookup> lookups =
          Iterables.transform(referencingIndexes, new Function<EntityIndex, IndexLookup>() {
            @Override
            public IndexLookup apply(EntityIndex index) {
              return index.lookup(new Object[]{put.getId()});
            }
          });

      session.prefetchIndexLookups(lookups);
      SetMultimap<EntityIndex, String> foundIds = MultimapUtils.createMultimap();

      for (IndexLookup lookup : lookups) {
        foundIds.putAll(lookup.getIndex(), session.indexLookup(lookup));
      }

      session.prefetchObjects(foundIds.values());

      for (Map.Entry<EntityIndex, String> entry : foundIds.entries()) {
        String objectId = entry.getValue();
        Sync.ObjectWrapper.Builder objectWrapper = session.get(objectId).toBuilder();
        Wrappers.cascadeDeleteTo(objectWrapper, entry.getKey());
        session.put(objectWrapper.build());
      }
    }
  }
}
