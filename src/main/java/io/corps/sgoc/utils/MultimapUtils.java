package io.corps.sgoc.utils;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/21.
 * Copyrighted by Zach Collins 2014
 */
public class MultimapUtils {
  public static <T> Supplier<Set<T>> newEmptySetSupplier() {
    return new Supplier<Set<T>>() {
      @Override
      public Set<T> get() {
        return new HashSet<T>();
      }
    };
  }

  public static <K, V> SetMultimap<K, V> createMultimap() {
    return Multimaps.<K, V>newSetMultimap(Maps.<K, Collection<V>>newHashMap(),
        MultimapUtils.<V>newEmptySetSupplier());
  }
}
