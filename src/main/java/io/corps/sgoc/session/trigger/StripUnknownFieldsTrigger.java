package io.corps.sgoc.session.trigger;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class StripUnknownFieldsTrigger extends BeforePutTrigger {
  public StripUnknownFieldsTrigger() {
    super(1, Type.SYSTEM);
  }

  @Override
  public Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema entitySchema, Sync.ObjectWrapper beingPut,
                                      Sync.ObjectWrapper existing, Set<String> upcomingIds) throws IOException {
    return stripUnknownFields(beingPut);
  }

  private <T extends Message> T stripUnknownFields(T message) {
    Message.Builder builder = null;
    if (message.getUnknownFields().asMap().size() > 0) {
      builder = message.toBuilder();
      builder.setUnknownFields(UnknownFieldSet.getDefaultInstance());
    }

    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();

      if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        if (fieldDescriptor.isRepeated()) {
          for (Object elem : (List) entry.getValue()) {
            Message value = (Message) elem;
            builder = strip(message, builder, fieldDescriptor, value);
          }
        } else {
          Message value = (Message) entry.getValue();
          builder = strip(message, builder, fieldDescriptor, value);
        }
      }
    }

    if (builder != null) {
      return (T) builder.build();
    }

    return message;
  }

  private <T extends Message> Message.Builder strip(T message, Message.Builder builder,
                                                    Descriptors.FieldDescriptor fieldDescriptor, Message value) {
    Message stripped = stripUnknownFields(value);
    if (value != stripped) {
      if (builder == null) {
        builder = message.toBuilder();
      }

      builder.setField(fieldDescriptor, stripped);
    }
    return builder;
  }
}
