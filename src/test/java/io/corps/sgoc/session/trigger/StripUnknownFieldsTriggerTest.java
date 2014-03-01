package io.corps.sgoc.session.trigger;

import com.google.protobuf.UnknownFieldSet;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;
import io.corps.sgoc.testutils.Fixtures;
import org.junit.Assert;

import java.util.Collections;

/**
 * Created by corps@github.com on 2014/02/24.
 * Copyrighted by Zach Collins 2014
 */
public class StripUnknownFieldsTriggerTest {
  @org.junit.Test
  public void testBeforePut() throws Exception {
    StripUnknownFieldsTrigger trigger = new StripUnknownFieldsTrigger();

    UnknownFieldSet.Field someField = UnknownFieldSet.Field.getDefaultInstance();
    UnknownFieldSet someUnknownFields = UnknownFieldSet.newBuilder().addField(71, someField).build();
    Sync.ObjectWrapper apple =
        Fixtures.wrapAnApple(Fixtures.generateAnApple().setUnknownFields(someUnknownFields).build());
    Sync.ObjectWrapper result = trigger.beforePut(null, null, apple, null, Collections.<String>emptySet());

    Assert.assertEquals(result, apple.toBuilder().setExtension(Test.apple,
        apple.getExtension(Test.apple).toBuilder().setUnknownFields(UnknownFieldSet.getDefaultInstance()).build())
        .build());
  }
}
