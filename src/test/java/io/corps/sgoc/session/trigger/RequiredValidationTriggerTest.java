package io.corps.sgoc.session.trigger;

import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by corps@github.com on 2014/02/25.
 * Copyrighted by Zach Collins 2014
 */
public class RequiredValidationTriggerTest {

  private EntitySchema schema;
  private RequiredValidationTrigger trigger;

  @Before
  public void setUp() throws Exception {
    schema = new EntitySchema(Test.getDescriptor());
    trigger = new RequiredValidationTrigger();
  }

  @org.junit.Test
  public void testBeforePut() throws Exception {
    Sync.ObjectWrapper spaghetti = Fixtures.wrapASpaghetti(
        Fixtures.generateASpaghetti().addNoodle(Test.Spaghetti.Noodle.newBuilder().addType("Rubbery"))
            .setPlateName("abc").build());

    Assert.assertEquals(spaghetti, afterValidation(spaghetti));

    spaghetti = Fixtures.wrapASpaghetti(
        spaghetti.getExtension(Test.spaghetti).toBuilder()
            .addNoodle(Test.Spaghetti.Noodle.newBuilder().addType("Hoo").addType(""))
            .build());

    Assert.assertEquals(spaghetti.toBuilder().setDeleted(true).build(), afterValidation(spaghetti));

    spaghetti = Fixtures.wrapASpaghetti(Fixtures.generateAWrapper().build(),
        Test.Spaghetti.newBuilder().addDescriptiveWords("Hot pot spot").addDescriptiveWords("Yum yum")
            .setPlateName("vvvvv").build());
    Assert.assertEquals(spaghetti, afterValidation(spaghetti));

    spaghetti = Fixtures.wrapASpaghetti(Fixtures.generateAWrapper().build(),
        spaghetti.getExtension(Test.spaghetti).toBuilder().addDescriptiveWords("").setPlateName("hoho").build());
    Assert.assertEquals(spaghetti.toBuilder().setDeleted(true).build(), afterValidation(spaghetti));

    spaghetti = Fixtures.wrapASpaghetti(Fixtures.generateAWrapper().build(),
        Test.Spaghetti.newBuilder().addDescriptiveWords("Hot pot spot").addDescriptiveWords("Yum yum")
            .setPlateName("").build());
    Assert.assertEquals(spaghetti.toBuilder().setDeleted(true).build(), afterValidation(spaghetti));
  }

  private Sync.ObjectWrapper afterValidation(Sync.ObjectWrapper spaghetti) throws IOException {
    return trigger.beforePut(null, schema, spaghetti, null, Collections.<String>emptySet());
  }
}
