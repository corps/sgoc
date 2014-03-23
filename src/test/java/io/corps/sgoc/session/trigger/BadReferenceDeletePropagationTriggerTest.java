package io.corps.sgoc.session.trigger;

import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.session.ReadSession;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collections;

import static io.corps.sgoc.testutils.Fixtures.wrapAnApple;
import static io.corps.sgoc.testutils.ReferenceIds.idOf;
import static org.mockito.Mockito.*;

/**
 * Created by corps@github.com on 2014/02/23.
 * Copyrighted by Zach Collins 2014
 */
public class BadReferenceDeletePropagationTriggerTest {
  private EntitySchema schema;
  private BadReferenceDeletePropagationTrigger trigger;
  private ReadSession session;
  private Test.Apple.Builder appleBuilder;
  private Test.Pie.Builder pieBuilder;

  @org.junit.Test
  public void testIgnoresValidRelationships() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId(idOf("BasketId")).build());
    when(session.get("BasketId")).thenReturn(Fixtures.wrapABasket());

    Assert.assertEquals(beingPut, trigger.beforePut(session, schema, beingPut, null,
        Collections.<String>emptySet()));
  }

  @org.junit.Test
  public void testTriggersOnDeletedReference() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId(idOf("BasketId")).build());
    when(session.get("BasketId")).thenReturn(Fixtures.wrapABasket().toBuilder().setDeleted(true).build());

    Assert.assertEquals(beingPut.toBuilder().setDeleted(true).build(),
        trigger.beforePut(session, schema, beingPut, null, Collections.<String>emptySet()));
  }

  @org.junit.Test
  public void testTriggersOnNonExistentReference() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId(idOf("BasketId")).build());
    when(session.get("BasketId")).thenReturn(null);

    Assert.assertEquals(beingPut.toBuilder().setDeleted(true).build(),
        trigger.beforePut(session, schema, beingPut, null, Collections.<String>emptySet()));
  }

  @org.junit.Test
  public void testTriggersOnInvalidType() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId(idOf("BasketId")).build());
    when(session.get("BasketId")).thenReturn(Fixtures.wrapAnOrange());

    Assert.assertEquals(beingPut.toBuilder().setDeleted(true).build(),
        trigger.beforePut(session, schema, beingPut, null, Collections.<String>emptySet()));
  }

  @org.junit.Test
  public void testTriggerIgnoresUnset() throws Exception {
    Sync.ObjectWrapper beingPut = Fixtures.wrapAPie(pieBuilder.build());

    Assert.assertEquals(beingPut, trigger.beforePut(session, schema, beingPut, null,
        Collections.<String>emptySet()));
    verifyZeroInteractions(session);
  }

  @org.junit.Test
  public void testTriggerHandlesSetNull() throws Exception {
    Sync.ObjectWrapper beingPut = Fixtures.wrapAPie(pieBuilder.setFruitId(idOf("FruitId")).build());
    when(session.get("FruitId")).thenReturn(null);

    Assert.assertEquals(Fixtures.wrapAPie(beingPut, pieBuilder.clearFruitId().build()),
        trigger.beforePut(session, schema, beingPut, null, Collections.<String>emptySet()));
  }

  @Before
  public void setUp() throws Exception {
    schema = new EntitySchema(Test.getDescriptor());
    trigger = new BadReferenceDeletePropagationTrigger();
    session = mock(ReadSession.class);
    appleBuilder = Fixtures.generateAnApple();
    pieBuilder = Test.Pie.newBuilder();
  }
}
