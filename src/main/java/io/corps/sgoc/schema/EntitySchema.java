package io.corps.sgoc.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.SetMultimap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.TextFormat;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.utils.MultimapUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/21.
 * Copyrighted by Zach Collins 2014
 */
public class EntitySchema {
  private final Map<Descriptors.FieldDescriptor, PayloadEntity> payloadEntities;
  private final SetMultimap<PayloadEntity, EntityIndex> referencingIndexes;
  private final Descriptors.FileDescriptor fileDescriptor;

  public EntitySchema(Descriptors.FileDescriptor fileDescriptor) {
    this.fileDescriptor = fileDescriptor;
    referencingIndexes = MultimapUtils.createMultimap();
    payloadEntities = new HashMap<>();

    analyzeSchema();
  }

  public PayloadEntity getEntity(Descriptors.FieldDescriptor fieldDescriptor) {
    return payloadEntities.get(fieldDescriptor);
  }

  public Set<EntityIndex> getReferencingIndexes(PayloadEntity entity) {
    return referencingIndexes.get(entity);
  }

  public Descriptors.FieldDescriptor getPayloadDescriptorField(Sync.ObjectWrapper objectWrapper) {
    for (Descriptors.FieldDescriptor fieldDescriptor : payloadEntities.keySet()) {
      if (objectWrapper.hasField(fieldDescriptor)) {
        return fieldDescriptor;
      }
    }

    throw new IllegalArgumentException("Excepted to find a known payload on " +
        TextFormat.shortDebugString(objectWrapper));
  }

  public PayloadEntity getEntity(Sync.ObjectWrapper objectWrapper) {
    return getEntity(getPayloadDescriptorField(objectWrapper));
  }

  private void analyzeSchema() {
    for (Descriptors.FieldDescriptor fieldDescriptor : fileDescriptor.getExtensions()) {
      if (fieldDescriptor.getContainingType().equals(Sync.ObjectWrapper.getDescriptor())) {
        payloadEntities.put(fieldDescriptor, new PayloadEntity(fieldDescriptor));
      }
    }

    for (PayloadEntity payloadEntity : payloadEntities.values()) {
      for (EntityIndex entityIndex : payloadEntity.getIndexes()) {
        if (entityIndex.isReference()) {
          for (Descriptors.FieldDescriptor fieldDescriptor : entityIndex.getReferencedWrapperFields()) {
            Preconditions.checkArgument(payloadEntities.containsKey(fieldDescriptor),
                "Unknown entity wrapper field: " + TextFormat.shortDebugString(fieldDescriptor.toProto()));
            referencingIndexes.put(payloadEntities.get(fieldDescriptor), entityIndex);
          }
        }
      }
    }
  }
}
