package io.corps.sgoc.persistence.session.trigger;

import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class IdempotentDeleteTriggerTest {
  @Test
  public void testBeforePut() throws Exception {
    IdempotentDeleteTrigger trigger = new IdempotentDeleteTrigger();
    Sync.ObjectWrapper apple = Fixtures.wrapAnApple();
    Sync.ObjectWrapper deletedApple = apple.toBuilder().setDeleted(true).build();
    Sync.ObjectWrapper previous = Fixtures.wrapAnApple();

    Assert.assertEquals(apple, trigger.beforePut(null, null, apple, null));
    Assert.assertEquals(apple, trigger.beforePut(null, null, apple, previous));

    previous = previous.toBuilder().setDeleted(true).build();
    Assert.assertEquals(deletedApple, trigger.beforePut(null, null, apple, previous));

    apple = deletedApple;
    Assert.assertEquals(apple, trigger.beforePut(null, null, apple, null));
    Assert.assertEquals(apple, trigger.beforePut(null, null, apple, previous));
  }
}
