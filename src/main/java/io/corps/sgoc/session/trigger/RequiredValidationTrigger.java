package io.corps.sgoc.session.trigger;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import io.corps.sgoc.schema.Schema;
import io.corps.sgoc.sync.Sync;

/**
 * Created by corps@github.com on 2014/02/25.
 * Copyrighted by Zach Collins 2014
 */
public class RequiredValidationTrigger extends ValidationTrigger<Boolean> {
  public RequiredValidationTrigger() {
    super(Schema.required,
        ImmutableSet.<Descriptors.FieldDescriptor.Type>of(Descriptors.FieldDescriptor.Type.STRING,
            Descriptors.FieldDescriptor.Type.MESSAGE),
        true, 0);
  }

  @Override
  protected boolean isValid(Boolean isRequired, Descriptors.FieldDescriptor fieldDescriptor, Object object) {
    if (object instanceof Sync.ReferenceId) {
      return !isRequired || !Strings.isNullOrEmpty(((Sync.ReferenceId) object).getId());
    } else if (!(object instanceof String)) {
      throw new IllegalStateException("Cannot apply required to field of type " + object.getClass().toString());
    }
    return !isRequired || !Strings.isNullOrEmpty((String) object);
  }
}
