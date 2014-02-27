package io.corps.sgoc.schema;

import com.google.common.collect.Lists;
import io.corps.sgoc.sync.Sync;

import java.util.List;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public class IndexFieldPaths {
  private final List<FieldPath<Sync.ObjectWrapper>> fieldPaths;

  public IndexFieldPaths(FieldPath<Sync.ObjectWrapper> singleFieldPath) {
    fieldPaths = Lists.newArrayList();
    fieldPaths.add(singleFieldPath);
  }

  public IndexFieldPaths(List<FieldPath<Sync.ObjectWrapper>> fieldPaths) {
    this.fieldPaths = fieldPaths;
  }

  @Override
  public int hashCode() {
    return fieldPaths.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexFieldPaths that = (IndexFieldPaths) o;

    if (!fieldPaths.equals(that.fieldPaths)) return false;

    return true;
  }

  @Override
  public String toString() {
    return "IndexFieldPaths{" +
        "fieldPaths=" + fieldPaths +
        '}';
  }

  public List<FieldPath<Sync.ObjectWrapper>> getFieldPaths() {

    return fieldPaths;
  }
}
