package io.corps.sgoc.testutils;

import com.google.protobuf.ExtensionRegistry;
import io.corps.sgoc.session.config.SessionConfig;
import io.corps.sgoc.schema.Schema;
import io.corps.sgoc.test.model.Test;

/**
 * Created by corps@github.com on 2014/02/27.
 * Copyrighted by Zach Collins 2014
 */
public class TestSessionBuilder {
  public static SessionConfig.Builder configBuilder() {
    ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
    Schema.registerAllExtensions(extensionRegistry);
    Test.registerAllExtensions(extensionRegistry);
    return SessionConfig.builder().setRootModelFileDescriptor(Test.getDescriptor()).setExtensionRegistry(
        extensionRegistry);
  }
}
