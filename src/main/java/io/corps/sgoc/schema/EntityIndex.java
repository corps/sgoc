package io.corps.sgoc.schema;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.corps.sgoc.sync.Sync;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public class EntityIndex {
  public static final String REFERENCE_TYPE_ERROR_MESSAGE =
      "Reference types should be a single empty extension on the ObjectWrapper";
  private final Schema.IndexDescriptor indexDescriptor;
  private final List<FieldPath<Sync.ObjectWrapper>> fieldPaths;
  private final Schema.ReferenceDescriptor referenceDescriptor;

  EntityIndex(Schema.IndexDescriptor indexDescriptor,
              Schema.ReferenceDescriptor referenceDescriptor) {

    this.referenceDescriptor = referenceDescriptor;
    Preconditions.checkNotNull(indexDescriptor);

    this.fieldPaths = new ArrayList<>();
    for (Sync.ObjectWrapper objectWrapper : indexDescriptor.getKeyFieldList()) {
      this.fieldPaths.add(FieldPath.fieldPathOf(objectWrapper));
    }

    if (referenceDescriptor != null) {
      validateReferenceDescriptor();
    }

    this.indexDescriptor = indexDescriptor;
  }

  public Schema.IndexDescriptor getIndexDescriptor() {
    return indexDescriptor;
  }

  public boolean isUnique() {
    return indexDescriptor.getUnique();
  }

  public String getName() {
    return indexDescriptor.getName();
  }

  public boolean hasName() {
    return indexDescriptor.hasName();
  }

  public List<FieldPath<Sync.ObjectWrapper>> getFieldPaths() {
    return fieldPaths;
  }

  @Override
  public int hashCode() {
    int result = indexDescriptor.hashCode();
    result = 31 * result + (referenceDescriptor != null ? referenceDescriptor.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EntityIndex that = (EntityIndex) o;

    if (!indexDescriptor.equals(that.indexDescriptor)) return false;
    if (referenceDescriptor != null ? !referenceDescriptor.equals(that.referenceDescriptor) :
        that.referenceDescriptor != null) return false;

    return true;
  }

  @Override
  public String toString() {
    return "EntityIndex{" +
        "indexDescriptor=" + indexDescriptor +
        ", referenceDescriptor=" + referenceDescriptor +
        '}';
  }

  public boolean isReference() {
    return referenceDescriptor != null;
  }

  public IndexLookup lookup(Sync.ObjectWrapper objectWrapper) {
    return lookup(getIndexValues(objectWrapper));
  }

  public IndexLookup lookup(Object[] objects) {
    return new IndexLookup(this, objects);
  }

  public FieldPath<Sync.ObjectWrapper> getReferenceFieldPath() {
    Preconditions.checkState(isReference());
    return fieldPaths.get(0);
  }

  public Schema.ReferenceDescriptor.OnDeleteBehavior getCascadeDeletionBehavior() {
    Preconditions.checkState(isReference());
    return referenceDescriptor.getOnDelete();
  }

  public List<Descriptors.FieldDescriptor> getReferencedWrapperFields() {
    return Lists
        .transform(referenceDescriptor.getTypeList(), new Function<Sync.ObjectWrapper, Descriptors.FieldDescriptor>() {
          @Override
          public Descriptors.FieldDescriptor apply(Sync.ObjectWrapper input) {
            Map<Descriptors.FieldDescriptor, Object> fields = input.getAllFields();
            Preconditions.checkArgument(fields.size() == 1, REFERENCE_TYPE_ERROR_MESSAGE);
            Descriptors.FieldDescriptor wrapperField = fields.keySet().iterator().next();
            Preconditions.checkArgument(wrapperField.isExtension(),
                REFERENCE_TYPE_ERROR_MESSAGE);
            Preconditions.checkArgument(((Message) (input.getField(wrapperField))).getAllFields().size() == 0,
                REFERENCE_TYPE_ERROR_MESSAGE);
            return wrapperField;
          }
        });
  }

  private void validateReferenceDescriptor() {
    Preconditions.checkArgument(fieldPaths.size() == 1);
    FieldPath<Sync.ObjectWrapper> referencePath = fieldPaths.get(0);
    Descriptors.FieldDescriptor last = Iterators.getLast(referencePath.iterator(), null);
    last = Preconditions.checkNotNull(last);
    Preconditions.checkArgument(last.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.STRING));
    Preconditions.checkArgument(last.getContainingType().equals(Sync.ReferenceId.getDescriptor()));
  }

  private Object[] getIndexValues(Sync.ObjectWrapper objectWrapper) {
    objectWrapper = Preconditions.checkNotNull(objectWrapper);
    Object[] result = new Object[fieldPaths.size()];

    for (int i = 0; i < result.length; ++i) {
      FieldPath<Sync.ObjectWrapper> fieldPath = fieldPaths.get(i);
      result[i] = fieldPath.resolve(objectWrapper);
    }

    return result;
  }
}
