package io.corps.sgoc.persistence.session.trigger;

import io.corps.sgoc.persistence.session.ReadSession;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.utils.Wrappers;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/02/23.
 * Copyrighted by Zach Collins 2014
 */
// TEST EDGE CASE:  References valid object, but the object gets invalidated after the fact.
public class BadReferenceDeletePropagationTrigger extends BeforePutTrigger {
  public BadReferenceDeletePropagationTrigger() {
    super(0);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema entitySchema,
                                      Sync.ObjectWrapper beingPut,
                                      Sync.ObjectWrapper existing) throws IOException {
    if (beingPut.getDeleted()) {
      return beingPut;
    }

    Sync.ObjectWrapper.Builder replacement = null;

    PayloadEntity entity = entitySchema.getEntity(beingPut);
    for (EntityIndex index : entity.getIndexes()) {
      if (index.isReference()) {
        String fk = (String) index.getReferenceFieldPath().resolve(beingPut);

        if (fk == null || fk.isEmpty()) {
          continue;
        }

        Sync.ObjectWrapper referencedObject = session.get(beingPut.getId().toBuilder().setUuid(fk).build());
        if (doesNotExist(referencedObject) || invalidType(entitySchema, index, referencedObject)) {
          if (replacement == null) {
            replacement = beingPut.toBuilder();
          }
          Wrappers.cascadeDeleteTo(replacement, index);

          if (replacement.getDeleted()) break;
        }
      }
    }

    if (replacement != null) {
      return replacement.build();
    }
    return beingPut;
  }

  private boolean invalidType(EntitySchema entitySchema, EntityIndex index, Sync.ObjectWrapper referencedObject) {
    return !Wrappers.instanceOf(entitySchema, referencedObject, index.getReferencedWrapperFields());
  }

  private boolean doesNotExist(Sync.ObjectWrapper referencedObject) {
    return !Wrappers.isPresent(referencedObject);
  }
}
