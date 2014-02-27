package io.corps.sgoc.schema;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/20.
 * Copyrighted by Zach Collins 2014
 */
public class FieldPath<T extends GeneratedMessage> implements Iterable<Descriptors.FieldDescriptor> {
  private final List<Descriptors.FieldDescriptor> fieldDescriptors;

  public FieldPath(List<Descriptors.FieldDescriptor> fieldDescriptors) {
    this.fieldDescriptors = Preconditions.checkNotNull(fieldDescriptors);
  }

  public static <T extends GeneratedMessage> FieldPath<T> fieldPathOf(T pathOrigin) {
    List<Descriptors.FieldDescriptor> indexPath = new ArrayList<>();

    Message object = pathOrigin;
    Map<Descriptors.FieldDescriptor, Object> allFields = object.getAllFields();
    while (!allFields.isEmpty()) {
      Preconditions.checkArgument(allFields.size() == 1,
          "Field paths should only include non branching fields, and end on a primitive. Found this field path: " +
              object);

      Descriptors.FieldDescriptor next = allFields.keySet().iterator().next();
      indexPath.add(next);

      if (next.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
        object = (Message) object.getField(next);
        allFields = object.getAllFields();
      } else {
        break;
      }
    }
    return new FieldPath<>(indexPath);
  }

  public Object resolve(T message) {
    Object result = message;
    for (Descriptors.FieldDescriptor fieldDescriptor : this) {
      Message currentMessage = (Message) result;
      result = currentMessage.getField(fieldDescriptor);
    }

    return result;
  }

  @Override
  public int hashCode() {
    return fieldDescriptors.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FieldPath fieldPath = (FieldPath) o;

    if (!fieldDescriptors.equals(fieldPath.fieldDescriptors)) return false;

    return true;
  }

  @Override
  public String toString() {
    return "FieldPath{" +
        "fieldDescriptors=" + fieldDescriptors +
        '}';
  }

  @Override
  public Iterator<Descriptors.FieldDescriptor> iterator() {
    return fieldDescriptors.iterator();
  }

  public void buildKeyField(T.Builder keyObject) {
    Iterator<Descriptors.FieldDescriptor> iterator = iterator();
    if (iterator.hasNext()) {
      Descriptors.FieldDescriptor next = iterator.next();
      keyObject.setField(next, buildDefaultFor(keyObject, next, iterator));
    }
  }

  private Object buildDefaultFor(Message.Builder parent,
                                 Descriptors.FieldDescriptor currentField,
                                 Iterator<Descriptors.FieldDescriptor> iterator) {
    if (currentField.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
      Message.Builder nextBuilder =
          ((Message) parent.getField(currentField)).getDefaultInstanceForType().toBuilder();
      if (iterator.hasNext()) {
        Descriptors.FieldDescriptor next = iterator.next();
        nextBuilder.setField(next, buildDefaultFor(nextBuilder, next, iterator));
      }
      return nextBuilder.build();
    } else {
      Preconditions.checkState(!iterator.hasNext());
      return currentField.getDefaultValue();
    }
  }
}
