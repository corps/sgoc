package io.corps.sgoc.schema;

import com.google.protobuf.Descriptors;
import org.junit.Before;

/**
 * Created by corps@github.com on 2014/02/21.
 * Copyrighted by Zach Collins 2014
 */
public class EntitySchemaTest {
  private EntitySchema schema;
  private Descriptors.FileDescriptor descriptor;

  @Before
  public void setUp() throws Exception {
    descriptor = io.corps.sgoc.test.model.Test.getDescriptor();
    schema = new EntitySchema(descriptor);
  }
}
