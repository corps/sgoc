package io.corps.sgoc.session.trigger;

import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static io.corps.sgoc.testutils.ReferenceIds.idOf;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class ObjectVersioningTriggerTest {
  @Test
  public void testBeforePut() throws Exception {
    ObjectVersioningTrigger trigger = new ObjectVersioningTrigger();

    Sync.ObjectWrapper orange = Fixtures.wrapAnOrange();
    Assert.assertEquals(orange.toBuilder().setVersion(1).build(), trigger.beforePut(null, null, orange, null,
        Collections.<String>emptySet()));

    Sync.ObjectWrapper versionedOrange = orange.toBuilder().setVersion(123).build();
    Sync.ObjectWrapper coolOrange =
        Fixtures.wrapAnOrange(versionedOrange, Fixtures.generateAnOrange().setBasketId(idOf("Hohoho")).build());
    Assert.assertEquals(coolOrange.toBuilder().setVersion(124).build(),
        trigger.beforePut(null, null, coolOrange, versionedOrange, Collections.<String>emptySet()));

    Assert.assertEquals(versionedOrange, trigger.beforePut(null, null, orange, versionedOrange,
        Collections.<String>emptySet()));
  }
}
