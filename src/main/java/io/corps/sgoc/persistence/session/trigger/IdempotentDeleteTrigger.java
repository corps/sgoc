package io.corps.sgoc.persistence.session.trigger;

import io.corps.sgoc.persistence.session.ReadSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class IdempotentDeleteTrigger extends BeforePutTrigger {
  public IdempotentDeleteTrigger() {
    // Should likely occur before other triggers to prevent unnecessary processing of a deleted object,
    // and so that triggers could restore a write over a delete if it was deemed meaningful.
    super(-10000);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema entitySchema, Sync.ObjectWrapper beingPut,
                                      Sync.ObjectWrapper existing) throws IOException {
    if (existing != null && existing.getDeleted() && !beingPut.getDeleted()) {
      return beingPut.toBuilder().setDeleted(true).build();
    }

    return beingPut;
  }
}
