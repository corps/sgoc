package io.corps.sgoc.testutils;

import com.google.common.collect.Sets;
import org.mockito.ArgumentMatcher;

import java.util.Collection;

/**
 * Created by corps@github.com on 2014/02/20.
 * Copyrighted by Zach Collins 2014
 */
public class EqualWithoutRegardToOrder<T> extends ArgumentMatcher<Collection<T>> {
  private final Collection<T> one;

  public EqualWithoutRegardToOrder(Collection<T> one) {
    this.one = one;
  }

  @Override
  public boolean matches(Object other) {
    try {
      return Sets.newHashSet((Iterable)one).equals(Sets.newHashSet((Iterable)other));
    } catch(ClassCastException e) {
      return false;
    }
  }
}
