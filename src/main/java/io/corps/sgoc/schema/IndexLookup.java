package io.corps.sgoc.schema;

import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public class IndexLookup {
  private final EntityIndex index;
  private final Object[] objects;

  IndexLookup(EntityIndex index, Object[] objects) {
    this.index = Preconditions.checkNotNull(index);
    this.objects = Preconditions.checkNotNull(objects);
  }

  @Override
  public int hashCode() {
    int result = index.hashCode();
    result = 31 * result + Arrays.hashCode(objects);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexLookup that = (IndexLookup) o;

    if (!index.equals(that.index)) return false;
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(objects, that.objects)) return false;

    return true;
  }

  @Override
  public String toString() {
    return String.format("IndexLookup: %s %s", index, Arrays.toString(objects));
  }
}
