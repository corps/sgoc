package io.corps.sgoc.session.trigger;

import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Created by corps@github.com on 2014/03/23.
 * Copyrighted by Zach Collins 2014
 */
public class IdempotentTypeTriggerTest {
  @Test
  public void testBeforePut() throws Exception {
    IdempotentTypeTrigger trigger = new IdempotentTypeTrigger();
    Sync.ObjectWrapper apple = Fixtures.wrapAnApple();
    Sync.ObjectWrapper deletedApple = apple.toBuilder().setDeleted(true).build();
    Sync.ObjectWrapper orange = Fixtures.wrapAnOrange();
    Sync.ObjectWrapper deletedOrange = orange.toBuilder().setDeleted(true).build();
    EntitySchema schema = new EntitySchema(io.corps.sgoc.test.model.Test.getDescriptor());

    Assert.assertEquals(apple, trigger.beforePut(null, schema, apple, null, Collections.<String>emptySet()));
    Assert.assertEquals(apple, trigger.beforePut(null, schema, apple, deletedApple, Collections.<String>emptySet()));

    Assert.assertEquals(orange, trigger.beforePut(null, schema, apple, orange, Collections.<String>emptySet()));
    Assert.assertEquals(deletedOrange,
        trigger.beforePut(null, schema, apple, deletedOrange, Collections.<String>emptySet()));

    Assert.assertEquals(orange, trigger.beforePut(null, schema, apple, orange, Collections.<String>emptySet()));
    Assert.assertEquals(orange,
        trigger.beforePut(null, schema, deletedApple, orange, Collections.<String>emptySet()));
  }
}
