package io.corps.sgoc.persistence.session.trigger;

import io.corps.sgoc.persistence.session.ReadWriteSession;
import io.corps.sgoc.persistence.session.Session;
import io.corps.sgoc.schema.EntitySchema;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

/**
 * Created by corps@github.com on 2014/02/26.
 * Copyrighted by Zach Collins 2014
 */
public abstract class TransactionTrigger {
  public static Comparator<TransactionTrigger> BY_PRIORITY = new Comparator<TransactionTrigger>() {
    @Override
    public int compare(TransactionTrigger o1, TransactionTrigger o2) {
      if (o1.priority < o2.priority) {
        return -1;
      }
      return 1;
    }
  };
  private final int priority;

  public TransactionTrigger(int priority) {
    this.priority = priority;
  }

  public abstract void afterCommit(ReadWriteSession session, EntitySchema entitySchema,
                                   Collection<Sync.ObjectWrapper> values) throws IOException;

  public abstract void afterRollback(Session session, EntitySchema entitySchema,
                                     Collection<Sync.ObjectWrapper> values) throws IOException;
}
