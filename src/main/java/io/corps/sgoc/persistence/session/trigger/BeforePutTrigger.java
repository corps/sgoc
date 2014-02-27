package io.corps.sgoc.persistence.session.trigger;

import io.corps.sgoc.persistence.session.ReadSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by corps@github.com on 2014/02/26.
 * Copyrighted by Zach Collins 2014
 */
public abstract class BeforePutTrigger {
  public static Comparator<BeforePutTrigger> BY_PRIORITY = new Comparator<BeforePutTrigger>() {
    @Override
    public int compare(BeforePutTrigger o1, BeforePutTrigger o2) {
      if (o1.type.ordering == o2.type.ordering) {
        return o1.relativeOrdering < o2.relativeOrdering ? -1 : 1;
      }
      return o1.type.ordering < o2.type.ordering ? -1 : 1;
    }
  };
  private final int relativeOrdering;
  private final Type type;

  BeforePutTrigger(int relativeOrdering, Type type) {
    this.relativeOrdering = relativeOrdering;
    this.type = type;
  }

  public BeforePutTrigger(int relativeOrdering) {
    this(relativeOrdering, Type.TRANSFORMATION);
  }

  public abstract Sync.ObjectWrapper beforePut(ReadSession session,
                                      EntitySchema schema,
                                      Sync.ObjectWrapper proposed,
                                      Sync.ObjectWrapper existing) throws IOException;

  public enum Type {
    TRANSFORMATION(0),
    VALIDATION(1),
    SYSTEM(2);
    private final int ordering;

    Type(int ordering) {
      this.ordering = ordering;
    }
  }
}
