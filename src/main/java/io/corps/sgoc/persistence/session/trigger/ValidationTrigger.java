package io.corps.sgoc.persistence.session.trigger;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import io.corps.sgoc.persistence.session.ReadSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.FieldPathChain;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/26.
 * Copyrighted by Zach Collins 2014
 */
public abstract class ValidationTrigger<ExtensionType> extends BeforePutTrigger {
  private final GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, ExtensionType> option;
  private final Set<Descriptors.FieldDescriptor.Type> validTypes;
  private final boolean expandLists;

  public ValidationTrigger(GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, ExtensionType> option,
                           Set<Descriptors.FieldDescriptor.Type> validTypes,
                           boolean expandLists, int relativeOrdering) {
    super(relativeOrdering, Type.VALIDATION);
    this.option = option;
    this.validTypes = validTypes;
    this.expandLists = expandLists;
  }

  @Override
  public final Sync.ObjectWrapper beforePut(ReadSession session, EntitySchema schema, Sync.ObjectWrapper proposed,
                                      Sync.ObjectWrapper existing) throws IOException {
    if (proposed.getDeleted()) {
      return proposed;
    }

    PayloadEntity entity = schema.getEntity(proposed);
    for (FieldPathChain<Sync.ObjectWrapper> pathChain : entity.getFieldPathChainsFor(option)) {
      for (FieldPathChain<Sync.ObjectWrapper>.Pair pair : pathChain.iterate(proposed, expandLists)) {
        if (validTypes.contains(pair.fieldDescriptor.getType())) {
          if (!isValid(pair.fieldDescriptor.getOptions().getExtension(option), pair.fieldDescriptor, pair.object)) {
            return proposed.toBuilder().setDeleted(true).build();
          }
        }
      }
    }

    return proposed;
  }

  protected abstract boolean isValid(ExtensionType extension, Descriptors.FieldDescriptor fieldDescriptor,
                                     Object object);
}
