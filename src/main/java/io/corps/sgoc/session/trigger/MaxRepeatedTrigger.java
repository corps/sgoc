package io.corps.sgoc.session.trigger;

import com.google.protobuf.Descriptors;
import io.corps.sgoc.schema.Schema;

import java.util.List;

/**
 * Created by corps@github.com on 2014/02/27.
 * Copyrighted by Zach Collins 2014
 */
public class MaxRepeatedTrigger extends ValidationTrigger<Integer> {
  public MaxRepeatedTrigger() {
    super(Schema.maxRepeated, null, false, 0);
  }

  @Override
  protected boolean isValid(Integer maxRepeated, Descriptors.FieldDescriptor fieldDescriptor, Object object) {
    return !fieldDescriptor.isRepeated() || ((List) object).size() <= maxRepeated;
  }
}
