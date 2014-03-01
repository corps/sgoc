package io.corps.sgoc.session.trigger;

import io.corps.sgoc.session.ReadWriteSession;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by corps@github.com on 2014/02/26.
 * Copyrighted by Zach Collins 2014
 */
public abstract class AfterPutTrigger {
  public static Comparator<AfterPutTrigger> BY_PRIORITY = new Comparator<AfterPutTrigger>() {
    @Override
    public int compare(AfterPutTrigger o1, AfterPutTrigger o2) {
      if (o1.priority < o2.priority) {
        return -1;
      }
      return 1;
    }
  };
  private final int priority;

  public AfterPutTrigger(int priority) {
    this.priority = priority;
  }

  public abstract void afterPut(ReadWriteSession session, EntitySchema entitySchema, Sync.ObjectWrapper put,
                                Sync.ObjectWrapper previous) throws IOException;
}
