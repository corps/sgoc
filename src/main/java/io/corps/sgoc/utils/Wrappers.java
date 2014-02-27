package io.corps.sgoc.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/23.
 * Copyrighted by Zach Collins 2014
 */
public class Wrappers {
  public static boolean isPresent(Sync.ObjectWrapper objectWrapper) {
    return objectWrapper != null && !objectWrapper.getDeleted();
  }

  public static void cascadeDeleteTo(Sync.ObjectWrapper.Builder objectWrapper,
                                     EntityIndex referenceIndex) {
    Preconditions.checkArgument(referenceIndex.isReference());
    switch (referenceIndex.getCascadeDeletionBehavior()) {
      case CASCADE:
        objectWrapper.setDeleted(true);
        break;
      case SET_NULL:
        referenceIndex.getReferenceFieldPath().clear(objectWrapper);
        break;
    }
  }

  public static boolean instanceOf(EntitySchema schema, Sync.ObjectWrapper wrapper,
                                   Collection<Descriptors.FieldDescriptor> types) {
    return types.contains(schema.getPayloadDescriptorField(wrapper));
  }

  public static boolean validateFields(Message object,
                                       Predicate<Map.Entry<Descriptors.FieldDescriptor, Object>> predicate) {
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : object.getAllFields().entrySet()) {
      Descriptors.FieldDescriptor descriptor = entry.getKey();
      if (descriptor.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
        List objects = null;
        Object value = entry.getValue();
        if(descriptor.isRepeated()) {
          objects = (List) value;
        } else {
          objects = Lists.newArrayList(value);
        }
        for (Object o : objects) {
          if (!validateFields((Message) o, predicate)) {
            return false;
          }
        }
      } else {
        if (!predicate.apply(entry)) {
          return false;
        }
      }
    }

    return true;
  }
}
