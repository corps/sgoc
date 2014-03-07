package io.corps.sgoc.session.trigger;

import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;

import java.util.Collections;

/**
 * Created by corps@github.com on 2014/02/27.
 * Copyrighted by Zach Collins 2014
 */
public class MaxLengthTriggerTest {
  @org.junit.Test
  public void testIsValid() throws Exception {
    MaxLengthTrigger trigger = new MaxLengthTrigger();

    EntitySchema schema = new EntitySchema(Test.getDescriptor());

    Sync.ObjectWrapper sketti = Fixtures.wrapASpaghetti();
    Assert.assertEquals(sketti, trigger.beforePut(null, schema, sketti, null, Collections.<String>emptySet()));

    sketti = Fixtures
        .wrapASpaghetti(Test.Spaghetti.newBuilder()
            .addNoodle(Test.Spaghetti.Noodle.getDefaultInstance())
            .addNoodle(Test.Spaghetti.Noodle.newBuilder().addType("12345678"))
            .build());

    Assert.assertEquals(sketti, trigger.beforePut(null, schema, sketti, null, Collections.<String>emptySet()));

    sketti = Fixtures
        .wrapASpaghetti(Test.Spaghetti.newBuilder()
            .addNoodle(Test.Spaghetti.Noodle.getDefaultInstance())
            .addNoodle(Test.Spaghetti.Noodle.newBuilder().addType("123456789"))
            .build());

    Assert.assertEquals(sketti.toBuilder().setDeleted(true).build(), trigger.beforePut(null, schema, sketti, null,
        Collections.<String>emptySet()));

    sketti = Fixtures
        .wrapASpaghetti(Test.Spaghetti.newBuilder().setPlateName("bb").build());
    Assert.assertEquals(sketti, trigger.beforePut(null, schema, sketti, null, Collections.<String>emptySet()));

    sketti = Fixtures
        .wrapASpaghetti(Test.Spaghetti.newBuilder().setPlateName("WOOAAAAHHH").build());
    Assert.assertEquals(sketti.toBuilder().setDeleted(true).build(), trigger.beforePut(null, schema, sketti, null,
        Collections.<String>emptySet()));
  }
}
