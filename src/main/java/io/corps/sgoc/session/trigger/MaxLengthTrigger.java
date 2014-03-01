package io.corps.sgoc.session.trigger;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import io.corps.sgoc.schema.Schema;

/**
 * Created by corps@github.com on 2014/02/27.
 * Copyrighted by Zach Collins 2014
 */
public class MaxLengthTrigger extends ValidationTrigger<Integer> {
  public MaxLengthTrigger() {
    super(Schema.maxLength,
        ImmutableSet.<Descriptors.FieldDescriptor.Type>of(Descriptors.FieldDescriptor.Type.STRING), true, 0);
  }

  @Override
  protected boolean isValid(Integer maxLength, Descriptors.FieldDescriptor fieldDescriptor, Object object) {
    return ((String) object).length() <= maxLength;
  }
}
