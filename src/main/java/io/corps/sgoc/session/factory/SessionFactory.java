package io.corps.sgoc.session.factory;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/03/03.
 * Copyrighted by Zach Collins 2014
 */
public interface SessionFactory {
  public <T> T doWork(String rootKey, ReadWriteSessionWork<T> work) throws IOException;

  public <T> T doReadOnlyWork(String rootKey, ReadOnlySessionWork<T> work) throws IOException;
}
