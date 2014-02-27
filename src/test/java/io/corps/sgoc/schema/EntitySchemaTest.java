package io.corps.sgoc.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.corps.sgoc.test.model.Test.*;

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

  @Test
  public void testGetEntity() throws Exception {
    Assert.assertEquals(apple.getDescriptor(),
        schema.getEntity(Fixtures.wrapAnApple()).wrapperFieldDescriptor);
  }

  @Test
  public void testGetReferencingIndexes() throws Exception {
    PayloadEntity basketEntity = schema.getEntity(basket.getDescriptor());
    PayloadEntity orangeEntity = schema.getEntity(orange.getDescriptor());
    PayloadEntity appleEntity = schema.getEntity(apple.getDescriptor());

    EntityIndex orangeBasketIndex = Iterables.find(orangeEntity.getIndexes(), new Predicate<EntityIndex>() {
      @Override
      public boolean apply(EntityIndex input) {
        return input.isReference();
      }
    });
    Assert.assertEquals(
        Sets.<EntityIndex>newHashSet(appleEntity.getIndex("Basket Index"), orangeBasketIndex),
        schema.getReferencingIndexes(basketEntity));
  }
}
