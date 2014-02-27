package io.corps.sgoc.persistence.session.trigger;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import io.corps.sgoc.schema.Schema;

/**
 * Created by corps@github.com on 2014/02/25.
 * Copyrighted by Zach Collins 2014
 */
public class RequiredValidationTrigger extends ValidationTrigger<Boolean> {
  public RequiredValidationTrigger() {
    super(Schema.required, ImmutableSet.<Descriptors.FieldDescriptor.Type>of(Descriptors.FieldDescriptor.Type.STRING),
        true, 0);
  }

  @Override
  protected boolean isValid(Boolean isRequired, Descriptors.FieldDescriptor fieldDescriptor, Object object) {
    return !isRequired || !Strings.isNullOrEmpty((String) object);
  }
}
