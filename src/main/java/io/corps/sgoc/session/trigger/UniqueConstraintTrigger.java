package io.corps.sgoc.session.trigger;

import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.session.exceptions.DependentPutReorderingRequest;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/03/05.
 * Copyrighted by Zach Collins 2014
 */
public class UniqueConstraintTrigger extends BeforePutTrigger {
  public UniqueConstraintTrigger() {
    super(-3);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema entitySchema,
                                      Sync.ObjectWrapper beingPut,
                                      Sync.ObjectWrapper existing, Set<String> upcomingIds) throws IOException {
    if (beingPut.getDeleted()) {
      return beingPut;
    }

    for (EntityIndex entityIndex : entitySchema.getEntity(beingPut).getIndexes()) {
      if (entityIndex.isUnique()) {
        Set<String> objectIds = session.indexLookup(entityIndex.lookup(beingPut));
        for (String objectId : objectIds) {
          if (!objectId.equals(beingPut.getId())) {
            if(upcomingIds.contains(objectId)) {
              throw new DependentPutReorderingRequest(objectId);
            }
            return beingPut.toBuilder().setDeleted(true).build();
          }
        }
      }
    }

    return beingPut;
  }
}
