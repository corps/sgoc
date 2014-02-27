package io.corps.sgoc.persistence.session.trigger;

import io.corps.sgoc.persistence.session.ReadSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Before;

import static io.corps.sgoc.testutils.Fixtures.wrapAnApple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId("BasketId").build());
    when(session.get(beingPut.getId().toBuilder().setUuid("BasketId").build())).thenReturn(Fixtures.wrapABasket());

    Assert.assertEquals(beingPut, trigger.beforePut(session, schema, beingPut, null));
  }

  @org.junit.Test
  public void testTriggersOnDeletedReference() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId("BasketId").build());
    when(session.get(beingPut.getId().toBuilder().setUuid("BasketId").build()))
        .thenReturn(Fixtures.wrapABasket().toBuilder().setDeleted(true).build());

    Assert.assertEquals(beingPut.toBuilder().setDeleted(true).build(),
        trigger.beforePut(session, schema, beingPut, null));
  }

  @org.junit.Test
  public void testTriggersOnNonExistentReference() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId("BasketId").build());
    when(session.get(beingPut.getId().toBuilder().setUuid("BasketId").build()))
        .thenReturn(null);

    Assert.assertEquals(beingPut.toBuilder().setDeleted(true).build(),
        trigger.beforePut(session, schema, beingPut, null));
  }

  @org.junit.Test
  public void testTriggersOnInvalidType() throws Exception {
    Sync.ObjectWrapper beingPut = wrapAnApple(appleBuilder.setBasketId("BasketId").build());
    when(session.get(beingPut.getId().toBuilder().setUuid("BasketId").build()))
        .thenReturn(Fixtures.wrapAnOrange());

    Assert.assertEquals(beingPut.toBuilder().setDeleted(true).build(),
        trigger.beforePut(session, schema, beingPut, null));
  }

  @org.junit.Test
  public void testTriggerIgnoresUnset() throws Exception {
    Sync.ObjectWrapper beingPut = Fixtures.wrapAPie(pieBuilder.build());

    Assert.assertEquals(beingPut, trigger.beforePut(session, schema, beingPut, null));
    verifyZeroInteractions(session);
  }

  @org.junit.Test
  public void testTriggerHandlesSetNull() throws Exception {
    Sync.ObjectWrapper beingPut = Fixtures.wrapAPie(pieBuilder.setFruitId("FruitId").build());
    when(session.get(beingPut.getId().toBuilder().setUuid("FruitId").build())).thenReturn(null);

    Assert.assertEquals(Fixtures.wrapAPie(beingPut, pieBuilder.clearFruitId().build()),
        trigger.beforePut(session, schema, beingPut, null));
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
