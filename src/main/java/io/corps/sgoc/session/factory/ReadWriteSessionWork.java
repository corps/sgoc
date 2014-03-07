package io.corps.sgoc.session.factory;

import io.corps.sgoc.session.ReadWriteSession;
import io.corps.sgoc.session.exceptions.WriteContentionException;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/03/03.
 * Copyrighted by Zach Collins 2014
 */
public interface ReadWriteSessionWork<T> {
  public T doWork(ReadWriteSession session) throws IOException, WriteContentionException;
}
