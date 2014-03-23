package io.corps.sgoc.schema;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Test;

import static io.corps.sgoc.test.model.Test.*;
import static io.corps.sgoc.testutils.ReferenceIds.idOf;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public class EntityIndexTest {
  @Test
  public void testGetReferencedWrapperFields() throws Exception {
    Sync.ObjectWrapper.Builder builder = Sync.ObjectWrapper.newBuilder();
    EntityIndex entityIndex =
        new EntityIndex(
            Schema.IndexDescriptor.newBuilder().addKeyField(Sync.ObjectWrapper.newBuilder()
                .setExtension(io.corps.sgoc.test.model.Test.apple,
                    Apple.newBuilder().setBasketId(idOf("")).build())
                .build())
                .build(),
            Schema.ReferenceDescriptor.newBuilder()
                .addType(builder.setExtension(apple, Apple.getDefaultInstance()))
                .addType(builder.clear().setExtension(orange, Orange.getDefaultInstance()))
                .addType(builder.clear().setExtension(basket, Basket.getDefaultInstance())).build());

    Assert.assertEquals(Lists.<Descriptors.FieldDescriptor>newArrayList(apple.getDescriptor(), orange.getDescriptor(),
        basket.getDescriptor()), entityIndex.getReferencedWrapperFields());

  }

  @Test
  public void testLookup() throws Exception {
    Schema.IndexDescriptor indexDescriptor =
        io.corps.sgoc.test.model.Test.apple.getDescriptor().getMessageType().getOptions().getExtension(Schema.entity)
            .getIndexList().get(0);

    EntityIndex entityIndex = new EntityIndex(indexDescriptor, null);
    IndexLookup lookup =
        entityIndex
            .lookup(Fixtures.wrapAnApple(Apple.newBuilder().setBasketId(idOf("123Basket")).setOrdinal(44).build()));

    Assert.assertArrayEquals(new Object[]{44L, "123Basket"}, lookup.getValues());
    Assert.assertEquals(entityIndex, lookup.getIndex());
  }
}
