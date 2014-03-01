package io.corps.sgoc.session.trigger;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/23.
 * Copyrighted by Zach Collins 2014
 */
public class ClearDeletedPayloadsTrigger extends BeforePutTrigger {
  public ClearDeletedPayloadsTrigger() {
    super(0, Type.SYSTEM);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema entitySchema, Sync.ObjectWrapper beingPut,
                                      Sync.ObjectWrapper existing, Set<String> upcomingIds) {
    if (beingPut.getDeleted()) {
      Descriptors.FieldDescriptor field = entitySchema.getPayloadDescriptorField(beingPut);
      if (beingPut.hasField(field)) {
        return beingPut.toBuilder()
            .setField(field, ((Message) beingPut.getField(field)).getDefaultInstanceForType()).build();
      }
    }
    return beingPut;
  }
}
