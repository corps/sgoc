package io.corps.sgoc.session.trigger;

import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class ObjectVersioningTrigger extends BeforePutTrigger {
  public ObjectVersioningTrigger() {
    super(10000, Type.SYSTEM);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema entitySchema, Sync.ObjectWrapper beingPut,
                                      Sync.ObjectWrapper existing, Set<String> upcomingIds) throws IOException {
    if (existing != null && existing.getVersion() > beingPut.getVersion()) {
      return existing;
    }

    return beingPut.toBuilder().setVersion(beingPut.getVersion() + 1).build();
  }
}
