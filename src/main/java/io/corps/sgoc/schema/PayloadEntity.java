package io.corps.sgoc.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.TextFormat;
import io.corps.sgoc.sync.Sync;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public class PayloadEntity {
  final Descriptors.FieldDescriptor wrapperFieldDescriptor;
  private final Map<Schema.IndexDescriptor, EntityIndex> indexes;
  private final Map<String, EntityIndex> indexByName;

  PayloadEntity(Descriptors.FieldDescriptor wrapperFieldDescriptor) {
    Preconditions.checkArgument(wrapperFieldDescriptor.isExtension() &&
        wrapperFieldDescriptor.getContainingType().equals(Sync.ObjectWrapper.getDescriptor()) &&
        wrapperFieldDescriptor.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE),
        "PayloadEntity must be a message extension on ObjectWrapper. Found: " +
            TextFormat.shortDebugString(wrapperFieldDescriptor.toProto()));

    this.wrapperFieldDescriptor = wrapperFieldDescriptor;
    this.indexes = new HashMap<>();
    this.indexByName = new HashMap<>();

    analyze();
  }

  public EntityIndex getIndex(String name) {
    return indexByName.get(name);
  }

  public EntityIndex getIndex(Schema.IndexDescriptor indexDescriptor) {
    return indexes.get(indexDescriptor);
  }

  @Override
  public int hashCode() {
    return wrapperFieldDescriptor.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PayloadEntity) {
      return ((PayloadEntity) obj).wrapperFieldDescriptor.equals(wrapperFieldDescriptor);
    }
    return false;
  }

  public Collection<EntityIndex> getIndexes() {
    return indexes.values();
  }

  private void analyze() {
    analyzeEntity();
    for (Descriptors.FieldDescriptor fieldDescriptor : wrapperFieldDescriptor.getMessageType().getFields()) {
      analyzeReference(fieldDescriptor);
    }
  }

  private void analyzeEntity() {
    Schema.EntityDescriptor entityDescriptor =
        wrapperFieldDescriptor.getMessageType().getOptions().getExtension(Schema.entity);
    analyzeIndexes(entityDescriptor);
  }

  private void analyzeIndexes(Schema.EntityDescriptor entityDescriptor) {
    for (Schema.IndexDescriptor indexDescriptor : entityDescriptor.getIndexList()) {
      addIndex(indexDescriptor, null);
    }
  }

  private void addIndex(Schema.IndexDescriptor indexDescriptor, Schema.ReferenceDescriptor referenceDescriptor) {
    EntityIndex index = new EntityIndex(indexDescriptor, referenceDescriptor);

    // Ideally would use conditionals and not generate the string everytime, but it's a minor setup time cost.
    String debugString = TextFormat.shortDebugString(indexDescriptor);
    Preconditions.checkArgument(!indexes.containsKey(indexDescriptor), "Identical indexes found: " + debugString);

    indexes.put(indexDescriptor, index);

    if (indexDescriptor.hasName()) {
      String name = indexDescriptor.getName();
      Preconditions.checkArgument(!name.isEmpty(), "Index cannot have empty string name set: " + debugString);
      Preconditions.checkArgument(!indexByName.containsKey(name), "Duplicate index with key name: " + debugString);
      indexByName.put(name, index);
    }
  }

  private void analyzeReference(Descriptors.FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.getOptions().hasExtension(Schema.reference)) {
      Schema.ReferenceDescriptor referenceDescriptor = fieldDescriptor.getOptions().getExtension(Schema.reference);
      Schema.IndexDescriptor newIndex = buildADefaultIndexDescriptor(fieldDescriptor, referenceDescriptor);

      EntityIndex existingIndex;
      if (referenceDescriptor.hasIndexName()) {
        existingIndex = indexByName.get(referenceDescriptor.getIndexName());
      } else {
        existingIndex = indexes.get(newIndex);
      }

      // Remove and update the index.
      if (existingIndex != null) {
        indexByName.remove(existingIndex.getIndexDescriptor().getName());
        indexes.remove(existingIndex.getIndexDescriptor());

        if (!existingIndex.getIndexDescriptor().getKeyFieldList().isEmpty()) {
          Preconditions.checkArgument(
              newIndex.getKeyFieldList().equals(existingIndex.getIndexDescriptor().getKeyFieldList()),
              "Reference is incompatible with is own index: " + TextFormat.shortDebugString(
                  existingIndex.getIndexDescriptor()));
        } else {
          // Merge in the key fields.
          newIndex =
              existingIndex.getIndexDescriptor().toBuilder().clearKeyField().addAllKeyField(newIndex.getKeyFieldList())
                  .build();
        }
      }

      addIndex(newIndex, referenceDescriptor);
    }
  }

  private Schema.IndexDescriptor buildADefaultIndexDescriptor(Descriptors.FieldDescriptor fieldDescriptor,
                                                              Schema.ReferenceDescriptor referenceDescriptor) {
    FieldPath<Sync.ObjectWrapper> fieldPath = new FieldPath<>(
        Lists.<Descriptors.FieldDescriptor>newArrayList(wrapperFieldDescriptor, fieldDescriptor));

    Schema.IndexDescriptor.Builder indexDescriptor = Schema.IndexDescriptor.newBuilder();
    Sync.ObjectWrapper.Builder keyObject = Sync.ObjectWrapper.newBuilder();
    fieldPath.buildKeyField(keyObject);
    indexDescriptor.addKeyField(keyObject);
    if (referenceDescriptor.hasIndexName()) {
      indexDescriptor.setName(referenceDescriptor.getIndexName());
    }
    return indexDescriptor.build();
  }
}