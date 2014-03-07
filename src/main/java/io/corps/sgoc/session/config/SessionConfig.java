package io.corps.sgoc.session.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import io.corps.sgoc.session.trigger.*;
import io.corps.sgoc.schema.EntitySchema;

import java.util.List;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/27.
 * Copyrighted by Zach Collins 2014
 */
public class SessionConfig {
  final EntitySchema entitySchema;
  final ExtensionRegistry extensionRegistry;
  final Set<BeforePutTrigger> beforePutTriggers;
  final Set<AfterPutTrigger> afterPutTriggers;
  final Set<TransactionTrigger> transactionTriggers;
  final int numRetries;

  private SessionConfig(Builder builder) {
    if (builder.rootModelFileDescriptor == null) {
      throw new IllegalArgumentException(
          "SessionConfig.Builder must be provided a FileDescriptor to build the schema!");
    }

    if (builder.extensionRegistry == null) {
      throw new IllegalArgumentException(
          "SessionConfig.Builder must be provided an extension register to deserialize object wrappers with!");
    }

    this.extensionRegistry = builder.extensionRegistry;
    this.beforePutTriggers = Sets.newTreeSet(BeforePutTrigger.BY_PRIORITY);
    this.afterPutTriggers = Sets.newTreeSet(AfterPutTrigger.BY_PRIORITY);
    this.transactionTriggers = Sets.newTreeSet(TransactionTrigger.BY_PRIORITY);
    this.entitySchema = new EntitySchema(builder.rootModelFileDescriptor);

    this.beforePutTriggers.addAll(builder.customBeforeTriggers);
    this.afterPutTriggers.addAll(builder.customAfterTriggers);
    this.transactionTriggers.addAll(builder.customTransactionTriggers);

    this.numRetries = builder.numRetries;

    if (builder.cascadeDeletes) {
      beforePutTriggers.add(new BadReferenceDeletePropagationTrigger());
      afterPutTriggers.add(new OnDeletePropagationTrigger());
    }

    if (builder.ensureDeleteIdempotence) {
      beforePutTriggers.add(new IdempotentDeleteTrigger());
    }

    if (builder.enableDefaultValidations) {
      beforePutTriggers.add(new MaxRepeatedTrigger());
      beforePutTriggers.add(new MaxLengthTrigger());
      beforePutTriggers.add(new RequiredValidationTrigger());
      beforePutTriggers.add(new UniqueConstraintTrigger());
    }

    if (builder.useOptimisticVersioning) {
      beforePutTriggers.add(new ObjectVersioningTrigger());
    }

    if (builder.stripUnknownFields) {
      beforePutTriggers.add(new StripUnknownFieldsTrigger());
    }

    beforePutTriggers.add(new ClearDeletedPayloadsTrigger());
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getNumRetries() {
    return numRetries;
  }

  public Set<BeforePutTrigger> getBeforePutTriggers() {
    return beforePutTriggers;
  }

  public Set<AfterPutTrigger> getAfterPutTriggers() {
    return afterPutTriggers;
  }

  public Set<TransactionTrigger> getTransactionTriggers() {
    return transactionTriggers;
  }

  public ExtensionRegistry getExtensionRegistry() {
    return extensionRegistry;
  }

  public EntitySchema getEntitySchema() {
    return entitySchema;
  }

  public static class Builder {
    private final List<BeforePutTrigger> customBeforeTriggers = Lists.newArrayList();
    private final List<AfterPutTrigger> customAfterTriggers = Lists.newArrayList();
    private final List<TransactionTrigger> customTransactionTriggers = Lists.newArrayList();
    private int numRetries = 5;
    private boolean ensureDeleteIdempotence = true;
    private boolean cascadeDeletes = true;
    private boolean enableDefaultValidations = true;
    private boolean stripUnknownFields = true;
    private boolean useOptimisticVersioning = true;
    private Descriptors.FileDescriptor rootModelFileDescriptor;
    private ExtensionRegistry extensionRegistry;

    public Builder setExtensionRegistry(ExtensionRegistry extensionRegistry) {
      this.extensionRegistry = extensionRegistry;
      return this;
    }

    public Builder setRootModelFileDescriptor(Descriptors.FileDescriptor rootModelFileDescriptor) {
      this.rootModelFileDescriptor = rootModelFileDescriptor;
      return this;
    }

    public Builder setNumRetries(int numRetries) {
      this.numRetries = numRetries;
      return this;
    }

    public Builder setUseOptimisticVersioning(boolean useOptimisticVersioning) {
      this.useOptimisticVersioning = useOptimisticVersioning;
      return this;
    }

    public Builder setStripUnknownFields(boolean stripUnknownFields) {
      this.stripUnknownFields = stripUnknownFields;
      return this;
    }

    public Builder setEnableDefaultValidations(boolean enableDefaultValidations) {
      this.enableDefaultValidations = enableDefaultValidations;
      return this;
    }

    public Builder setCascadeDeletes(boolean cascadeDeletes) {
      this.cascadeDeletes = cascadeDeletes;
      return this;
    }

    public Builder setEnsureDeleteIdempotence(boolean ensureDeleteIdempotence) {
      this.ensureDeleteIdempotence = ensureDeleteIdempotence;
      return this;
    }

    public SessionConfig build() {
      return new SessionConfig(this);
    }
  }
}
