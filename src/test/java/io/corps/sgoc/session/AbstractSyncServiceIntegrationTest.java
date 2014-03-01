package io.corps.sgoc.session;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ExtensionRegistry;
import io.corps.sgoc.schema.Schema;
import io.corps.sgoc.session.config.SessionConfig;
import io.corps.sgoc.session.factory.SessionFactory;
import io.corps.sgoc.session.trigger.ClearDeletedPayloadsTrigger;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.sync.SyncService;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.corps.sgoc.test.model.Test.Orange.Skin;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public abstract class AbstractSyncServiceIntegrationTest {

  public static final Comparator<Sync.ObjectWrapper> BY_OBJECT_ID = new Comparator<Sync.ObjectWrapper>() {
    @Override
    public int compare(Sync.ObjectWrapper o1, Sync.ObjectWrapper o2) {
      return o1.getId().compareTo(o2.getId());
    }
  };
  protected ExtensionRegistry extensionRegistry;
  private SessionConfig sessionConfig;
  private SyncService syncService;
  private SessionFactory sessionFactory;

  @BeforeClass
  public static void setupDb() throws Exception {
    SgocTestDatabase.setup().close();
  }

  @Before
  public void setup() throws Exception {
    extensionRegistry = ExtensionRegistry.newInstance();
    Schema.registerAllExtensions(extensionRegistry);
    Test.registerAllExtensions(extensionRegistry);
    sessionConfig = SessionConfig.builder().setExtensionRegistry(extensionRegistry)
        .setRootModelFileDescriptor(Test.getDescriptor())
        .build();
    sessionFactory = getSessionFactory(sessionConfig);
    syncService = getSyncService(sessionConfig, sessionFactory);
  }

  @After
  public void tearDown() throws Exception {
    close();
  }

  @org.junit.Test
  public void testPerformance() throws Exception {
    List<Sync.ObjectWrapper> payload1 = Lists.newArrayList();

    Random random = new Random();
    for (int i = 0; i < 3000; ++i) {
      Sync.ObjectWrapper basket = Fixtures.wrapABasket();
      payload1.add(basket);
      Sync.ObjectWrapper apple = Fixtures.wrapAnApple(
          Test.Apple.newBuilder().setOrdinal(random.nextInt(100)).setBasketId(basket.getId()).build());
      payload1.add(apple);
      payload1.add(Fixtures.wrapAnOrange(
          Test.Orange.newBuilder().setSkin(Skin.newBuilder().setTexture(Fixtures.generateUUID())).build()));
      Sync.ObjectWrapper pie = Fixtures.wrapAPie(Test.Pie.newBuilder().setFruitId(apple.getId()).build());
      payload1.add(pie);
    }

    List<Sync.ObjectWrapper> payload2 =
        Lists.newArrayList(Iterables.transform(Iterables.filter(payload1, new Predicate<Sync.ObjectWrapper>() {
          @Override
          public boolean apply(Sync.ObjectWrapper input) {
            return new Random().nextBoolean();
          }
        }), new Function<Sync.ObjectWrapper, Sync.ObjectWrapper>() {
          @Override
          public Sync.ObjectWrapper apply(Sync.ObjectWrapper input) {
            return input.toBuilder().setVersion(1).build();
          }
        }));

    Sync.PutRequest.Builder putBuilder = Sync.PutRequest.newBuilder().setRootId(Fixtures.generateUUID());
    Sync.PutRequest put1 = putBuilder.addAllObject(payload1).build();

    putBuilder.clearObject();
    Sync.PutRequest put2 = putBuilder.addAllObject(payload2).build();

    long maxTime = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    long startingTime = threadMXBean.getCurrentThreadCpuTime();
    syncService.put(put1);
    syncService.put(put2);
    long timeSpent = threadMXBean.getCurrentThreadCpuTime() - startingTime;
    System.out.println("Time was " + TimeUnit.MILLISECONDS.convert(timeSpent, TimeUnit.NANOSECONDS) + "ms");
    System.out.println("Objects modified " + payload1.size() + " & " + payload2.size());
    Assert.assertTrue("Spent " + timeSpent + " nanoseconds!", timeSpent < maxTime);
  }

  @org.junit.Test
  public void testSyncService() throws Exception {
    // Run the same test twice against two users to validate scoping of data
    for (String rootKey : new String[]{Fixtures.generateUUID(), Fixtures.generateUUID()}) {
      long thisVersion = 0;
      long lastVersion = 0;

      Sync.GetResponse getResponse = doGet(rootKey, thisVersion);
      Assert.assertTrue(getResponse.getChangedObjectList().isEmpty());
      Assert.assertTrue(getResponse.hasCurrentVersion());
      thisVersion = getResponse.getCurrentVersion();

      List<Sync.ObjectWrapper> expected = Lists.newArrayList();
      ArrayList<Sync.ObjectWrapper> changes = Lists.newArrayList();

      // Tests an object with a field.
      Sync.ObjectWrapper basket = Fixtures.wrapABasket(Fixtures.generateABasket().setName("Basket 1"));
      changes.add(basket);
      expected.add(bumpVersion(basket, 1));

      // Tests an object not having a field set (the return value being the same)
      Sync.ObjectWrapper basket1 = Fixtures.wrapABasket(Fixtures.generateABasket());
      changes.add(basket1);
      expected.add(bumpVersion(basket1, 1));

      // Tests an object with an embedded message and index.
      Sync.ObjectWrapper orange = Fixtures
          .wrapAnOrange(Fixtures.generateAnOrange().setSkin(Skin.newBuilder().setTexture("Bumpy")).build());
      changes.add(orange);
      expected.add(bumpVersion(orange, 1));

      // Tests an object successfully referencing an object earlier in the same transaction.
      Sync.ObjectWrapper pie =
          Fixtures.wrapAPie(Fixtures.generateAPie().setFruitId(orange.getId()).build());
      changes.add(pie);
      expected.add(bumpVersion(pie, 1));

      // Tests an object with a non required reference missing.
      Sync.ObjectWrapper pie1 = Fixtures.wrapAPie(Fixtures.generateAPie().build());
      changes.add(pie1);
      expected.add(bumpVersion(pie1, 1));

      // tests an object with a reference set to a non existent bad value.
      Sync.ObjectWrapper pie2 = Fixtures.wrapAPie(Fixtures.generateAPie().setFruitId("Not an id").build());
      changes.add(pie2);
      pie2 = bumpVersion(pie2, 1).toBuilder()
          .setExtension(Test.pie, pie2.getExtension(Test.pie).toBuilder().clearFruitId().build()).build();
      expected.add(pie2);


      getResponse = testSync(rootKey, thisVersion, expected, changes);
      lastVersion = thisVersion;
      thisVersion = getResponse.getCurrentVersion();
      expected.clear();
      changes.clear();

      // Test changing to an invalid / deleted reference with an older version
      changes.add(pie.toBuilder().setExtension(Test.pie,
          pie.toBuilder().getExtension(Test.pie).toBuilder().setFruitId("Some bad id").build()).build());
      pie = bumpVersion(pie, 1); // actual version after last put.
      expected.add(pie);

      // Test adding two apples to the same ordinal of the same basket, and one against a different basket.
      Sync.ObjectWrapper apple =
          Fixtures.wrapAnApple(Test.Apple.newBuilder().setOrdinal(23).setBasketId(basket.getId()).build());
      changes.add(apple);
      expected.add(bumpVersion(apple, 1));
      Sync.ObjectWrapper apple1 =
          Fixtures.wrapAnApple(Test.Apple.newBuilder().setOrdinal(23).setBasketId(basket.getId()).build());
      changes.add(apple1);
      expected.add(bumpVersion(deleted(apple1), 1));

      // Test adding two apples to the same ordinal of no set basket reference.
      Sync.ObjectWrapper apple3 =
          Fixtures.wrapAnApple(Test.Apple.newBuilder().setOrdinal(23).build());
      changes.add(apple3);
      expected.add(bumpVersion(apple3, 1));
      Sync.ObjectWrapper apple4 =
          Fixtures.wrapAnApple(Test.Apple.newBuilder().setOrdinal(23).build());
      changes.add(apple4);
      expected.add(deleted(bumpVersion(apple4, 1)));

      // Test modifying an existing object
      orange = bumpVersion(orange, 1); // Actual expected value from before.
      orange = orange.toBuilder().setExtension(Test.orange,
          orange.getExtension(Test.orange).toBuilder().setSkin(Skin.newBuilder().setTexture("New Skin!")).build())
          .build();
      changes.add(orange);
      expected.add(bumpVersion(orange, 2));

      // For use later.
      Sync.ObjectWrapper basket2 = Fixtures.wrapABasket();
      changes.add(basket2);
      expected.add(bumpVersion(basket2, 1));

      getResponse = testSync(rootKey, thisVersion, expected, changes);
      lastVersion = thisVersion;
      thisVersion = getResponse.getCurrentVersion();
      expected.clear();
      changes.clear();

      // adding a new apple with the same ordinal as an existing one, but the existing one is later deleted.
      apple = bumpVersion(apple, 1);
      apple = apple.toBuilder().setDeleted(true).build();

      Sync.ObjectWrapper apple5 = Fixtures.wrapAnApple(apple.getExtension(Test.apple));

      expected.add(bumpVersion(deleted(apple), 2));
      expected.add(bumpVersion(apple5, 1));

      changes.add(apple5);
      changes.add(apple);

      // Test that unique indexes work across transactions.
      Sync.ObjectWrapper apple6 = Fixtures.wrapAnApple(apple3.getExtension(Test.apple));
      changes.add(apple6);
      expected.add(deleted(bumpVersion(apple6, 1)));

      // References valid object, but the object gets deleted after the fact.
      Sync.ObjectWrapper orange1 =
          Fixtures.wrapAnOrange(Test.Orange.newBuilder().setBasketId(basket2.getId()).build());
      changes.add(orange1);

      basket2 = bumpVersion(deleted(basket2), 1);
      changes.add(basket2);

      expected.add(bumpVersion(deleted(orange1), 2)); // Skips to second version due to cascade.
      expected.add(bumpVersion(basket2, 2));

      // Reference before it actually gets put.
      Sync.ObjectWrapper basket3 = Fixtures.wrapABasket();
      Sync.ObjectWrapper orange2 =
          Fixtures.wrapAnOrange(Test.Orange.newBuilder().setBasketId(basket3.getId()).build());
      changes.add(orange2);
      changes.add(basket3);
      expected.add(bumpVersion(orange2, 1));
      expected.add(bumpVersion(basket3, 1));

      // idempotent delete
      apple1 = bumpVersion(apple1, 1);
      changes.add(apple1.toBuilder().setDeleted(false).build());
      expected.add(deleted(bumpVersion(apple1, 2)));

      testSync(rootKey, thisVersion, expected, changes);
      changes.clear();

      expected.add(bumpVersion(apple3, 1));
      expected.add(bumpVersion(deleted(apple4), 1));
      expected.add(bumpVersion(orange, 2));
      expected.add(bumpVersion(pie, 1));

      testSync(rootKey, lastVersion, expected, changes);

      expected.add(bumpVersion(basket, 1));
      expected.add(bumpVersion(basket1, 1));
      expected.add(bumpVersion(pie1, 1));
      expected.add(bumpVersion(pie2, 1));

      testSync(rootKey, 0, Lists.newArrayList(Iterables.filter(expected, new Predicate<Sync.ObjectWrapper>() {
        @Override
        public boolean apply(Sync.ObjectWrapper input) {
          return !input.getDeleted();
        }
      })), changes);

      testSync(rootKey, -1, expected, changes);
    }
  }

  private Sync.GetResponse testSync(String rootKey, long thisVersion, List<Sync.ObjectWrapper> expected,
                                    ArrayList<Sync.ObjectWrapper> changes) throws IOException {
    Sync.GetResponse getResponse;
    doPut(rootKey, changes);
    getResponse = doGet(rootKey, thisVersion);
    List<Sync.ObjectWrapper> getResponseObjects = Lists.newArrayList(getResponse.getChangedObjectList());
    Collections.sort(getResponseObjects, BY_OBJECT_ID);
    Collections.sort(expected, BY_OBJECT_ID);
    Assert.assertEquals(expected, getResponseObjects);
    return getResponse;
  }

  private Sync.ObjectWrapper deleted(Sync.ObjectWrapper o) {
    return new ClearDeletedPayloadsTrigger()
        .beforePut(null, sessionConfig.getEntitySchema(), o.toBuilder().setDeleted(true).build(), null,
            Collections.<String>emptySet());
  }

  private Sync.ObjectWrapper bumpVersion(Sync.ObjectWrapper o, long version) {
    return o.toBuilder().setVersion(version).build();
  }

  private void doPut(String rootKey, ArrayList<Sync.ObjectWrapper> values) throws IOException {
    syncService.put(Sync.PutRequest.newBuilder().setRootId(rootKey)
        .addAllObject(values).build());
  }

  protected abstract SessionFactory getSessionFactory(SessionConfig sessionConfig) throws Exception;

  private Sync.GetResponse doGet(String rootKey, long lastAppliedVersion) throws IOException {
    return syncService.get(
        Sync.GetRequest.newBuilder().setAppliedVersion(lastAppliedVersion).setRootId(rootKey).build());
  }

  protected abstract SyncService getSyncService(SessionConfig config, SessionFactory sessionFactory) throws Exception;

  protected abstract void close() throws Exception;
}
