package io.corps.sgoc.testutils;

import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.test.model.Test;

import java.util.UUID;

/**
 * Created by corps@github.com on 2014/02/20.
 * Copyrighted by Zach Collins 2014
 */
public class Fixtures {
  public static Sync.ObjectWrapper wrapAnOrange() {
    return wrapAnOrange(generateAWrapper().build(), generateAnOrange().build());
  }

  private static Test.Orange.Builder generateAnOrange() {
    return Test.Orange.newBuilder().setSkin(Test.Orange.Skin.newBuilder().setTexture("Plumpy"));
  }

  public static Sync.ObjectWrapper wrapAnApple() {
    return wrapAnApple(generateAWrapper().build(), generateAnApple().build());
  }

  public static Sync.ObjectWrapper wrapAnOrange(Sync.ObjectWrapper objectWrapper) {
    return wrapAnOrange(objectWrapper, generateAnOrange().build());
  }

  public static Sync.ObjectWrapper wrapAnApple(Sync.ObjectWrapper objectWrapper) {
    return wrapAnApple(objectWrapper, generateAnApple().build());
  }

  public static Sync.ObjectWrapper wrapAnOrange(Test.Orange orange) {
    return wrapAnOrange(generateAWrapper().build(), orange);
  }

  public static Sync.ObjectWrapper wrapAnApple(Test.Apple apple) {
    return wrapAnApple(generateAWrapper().build(), apple);
  }

  public static Sync.ObjectWrapper wrapAnApple(Sync.ObjectWrapper objectWrapper, Test.Apple apple) {
    return objectWrapper.toBuilder().setExtension(Test.apple, apple).build();
  }

  public static Sync.ObjectWrapper wrapAnOrange(Sync.ObjectWrapper objectWrapper, Test.Orange orange) {
    return objectWrapper.toBuilder().setExtension(Test.orange, orange).build();
  }

  public static Test.Apple buildAnApple(Test.Apple apple) {
    return generateAnApple().mergeFrom(apple).build();
  }

  public static Sync.ObjectWrapper buildAWrapper(Sync.ObjectWrapper objectWrapper) {
    return generateAWrapper().mergeFrom(objectWrapper).build();
  }

  public static Sync.ObjectWrapper.Builder generateAWrapper() {
    return Sync.ObjectWrapper.newBuilder()
        .setId(Sync.ObjectId.newBuilder().setUuid(generateUUID()).setRootId(generateUUID()));
  }

  public static Test.Apple.Builder generateAnApple() {
    return Test.Apple.newBuilder().setOrdinal(1);
  }

  public static String generateUUID() {
    return UUID.randomUUID().toString();
  }

}
