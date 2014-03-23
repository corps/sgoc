package io.corps.sgoc.session.trigger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.corps.sgoc.schema.EntityIndex;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.schema.PayloadEntity;
import io.corps.sgoc.session.Session;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.EqualWithoutRegardToOrder;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Before;

import static io.corps.sgoc.testutils.ReferenceIds.idOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class OnDeletePropagationTriggerTest {
  private EntitySchema schema;
  private OnDeletePropagationTrigger trigger;
  private Session session;
  private Test.Apple.Builder appleBuilder;
  private Test.Orange.Builder orangeBuilder;
  private Test.Basket.Builder basketBuilder;

  // Test valid path, test valid path but previous exists as deleted
  // test put is deleted.

  @org.junit.Test
  public void testName() throws Exception {
    Sync.ObjectWrapper basket = Fixtures.wrapABasket(
        Sync.ObjectWrapper.newBuilder().setId("BasketId").setDeleted(true).build(),
        basketBuilder.build());

    PayloadEntity appleEntity = schema.getEntity(Test.apple.getDescriptor());
    PayloadEntity orangeEntity = schema.getEntity(Test.orange.getDescriptor());

    EntityIndex appleReferenceIndex = Iterables.find(appleEntity.getIndexes(), new Predicate<EntityIndex>() {
      @Override
      public boolean apply(EntityIndex input) {
        return input.isReference();
      }
    });

    EntityIndex orangeReferenceIndex = Iterables.find(orangeEntity.getIndexes(), new Predicate<EntityIndex>() {
      @Override
      public boolean apply(EntityIndex input) {
        return input.isReference();
      }
    });

    Sync.ObjectWrapper apple = Fixtures.wrapAnApple(appleBuilder.setBasketId(idOf("BasketId")).build());
    Sync.ObjectWrapper orange = Fixtures.wrapAnOrange(orangeBuilder.setBasketId(idOf("BasketId")).build());
    String appleId = apple.getId();
    String orangeId = orange.getId();

    when(session.indexLookup(appleReferenceIndex.lookup(new Object[]{"BasketId"})))
        .thenReturn(Sets.newHashSet(appleId));
    when(session.indexLookup(orangeReferenceIndex.lookup(new Object[]{"BasketId"})))
        .thenReturn(Sets.newHashSet(orangeId));

    when(session.get(appleId)).thenReturn(apple);
    when(session.get(orangeId)).thenReturn(orange);

    trigger.afterPut(session, schema, basket, null);

    verify(session).indexLookup(appleReferenceIndex.lookup(new Object[]{"BasketId"}));
    verify(session).indexLookup(orangeReferenceIndex.lookup(new Object[]{"BasketId"}));
    verify(session).prefetchObjects(argThat(new EqualWithoutRegardToOrder<>(Sets.newHashSet(appleId, orangeId))));
    verify(session).put(apple.toBuilder().setDeleted(true).build());
    verify(session).put(orange.toBuilder().setDeleted(true).build());
  }

  @Before
  public void setUp() throws Exception {
    schema = new EntitySchema(Test.getDescriptor());
    trigger = new OnDeletePropagationTrigger();
    session = mock(Session.class);
    appleBuilder = Fixtures.generateAnApple();
    orangeBuilder = Fixtures.generateAnOrange();
    basketBuilder = Fixtures.generateABasket();
  }
}
