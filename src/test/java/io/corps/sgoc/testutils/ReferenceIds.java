package io.corps.sgoc.testutils;

import io.corps.sgoc.sync.Sync;

/**
 * Created by corps@github.com on 2014/03/23.
 * Copyrighted by Zach Collins 2014
 */
public class ReferenceIds {
  public static Sync.ReferenceId idOf(String id) {
    return Sync.ReferenceId.newBuilder().setId(id).build();
  }
}
