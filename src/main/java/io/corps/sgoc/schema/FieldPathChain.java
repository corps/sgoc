package io.corps.sgoc.schema;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;

import java.util.List;

/**
 * Created by corps@github.com on 2014/02/25.
 * Copyrighted by Zach Collins 2014
 */
public class FieldPathChain<T extends GeneratedMessage> {
  private List<FieldPath> fieldPaths;

  public FieldPathChain(FieldPath<T> rootFieldPath, List<FieldPath> otherPaths) {
    fieldPaths = Lists.newArrayList();
    fieldPaths.add(rootFieldPath);

    FieldPath lastPath = rootFieldPath;
    for (FieldPath otherPath : otherPaths) {
      Preconditions
          .checkArgument(lastPath.getTerminatingDescriptor().getMessageType()
              .equals(otherPath.getRootDescriptor()), "Non contiguous chain.");
      lastPath = otherPath;
    }

    fieldPaths.addAll(otherPaths);
  }

  public Iterable<Pair> iterate(T root, boolean expandLists) {
    return iterate(0, root, expandLists);
  }

  private Iterable<Pair> iterate(int fieldPathIdx, GeneratedMessage root, boolean expandLists) {
    FieldPath fieldPath = fieldPaths.get(fieldPathIdx);
    final Descriptors.FieldDescriptor descriptor = fieldPath.getTerminatingDescriptor();
    Object value = fieldPath.resolve(root);
    if (descriptor.isRepeated()) {
      List<Object> values = (List<Object>) value;
      if (fieldPathIdx + 1 < fieldPaths.size()) {
        Iterable[] iterators = new Iterable[values.size()];
        for (int i = 0; i < values.size(); ++i) {
          iterators[i] = iterate(fieldPathIdx + 1, (GeneratedMessage) values.get(i), expandLists);
        }

        return Iterables.<Pair>concat(iterators);
      } else {
        if (expandLists)
          return Iterables.transform(values, new Function<Object, Pair>() {
            @Override
            public Pair apply(Object input) {
              return new Pair(descriptor, input);
            }
          });
        else
          return Lists.newArrayList(new Pair(descriptor, values));
      }
    } else {
      return Lists.newArrayList(new Pair(descriptor, value));
    }
  }


  public class Pair {
    public Pair(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
      this.fieldDescriptor = fieldDescriptor;
      this.object = object;
    }

    public Descriptors.FieldDescriptor fieldDescriptor;
    public Object object;
  }
}
