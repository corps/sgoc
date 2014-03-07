package io.corps.sgoc.session.factory;

import io.corps.sgoc.session.ReadSession;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/03/03.
 * Copyrighted by Zach Collins 2014
 */
public interface ReadOnlySessionWork<T> {
  public T doWork(ReadSession session) throws IOException;
}