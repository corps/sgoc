package io.corps.sgoc.session.trigger;

import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/03/23.
 * Copyrighted by Zach Collins 2014
 */
public class IdempotentTypeTrigger extends BeforePutTrigger {
  public IdempotentTypeTrigger() {
    // Should likely occur before other triggers to prevent unnecessary processing of a deleted object,
    // and so that triggers could restore a write over a delete if it was deemed meaningful.
    super(-10000);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema schema, Sync.ObjectWrapper proposed,
                                      Sync.ObjectWrapper existing, Set<String> upcomingIds) throws IOException {
    if (existing != null &&
        !schema.getPayloadDescriptorField(existing).equals(schema.getPayloadDescriptorField(proposed))) {
      return existing;
    }

    return proposed;
  }
}
