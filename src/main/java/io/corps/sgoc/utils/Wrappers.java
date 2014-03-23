package io.corps.sgoc.utils;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.util.Collection;

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
        // Special case -- we want to clear, not just the actual id, but the entire reference id.
        referenceIndex.getReferenceFieldPath().getParentPath().clear(objectWrapper);
        break;
    }
  }

  public static boolean instanceOf(EntitySchema schema, Sync.ObjectWrapper wrapper,
                                   Collection<Descriptors.FieldDescriptor> types) {
    return types.contains(schema.getPayloadDescriptorField(wrapper));
  }
}
