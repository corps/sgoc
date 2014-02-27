package io.corps.sgoc.persistence.session;

import io.corps.sgoc.sync.Sync;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public interface WriteSession {
  // Adds the given object to the mutated objects cache, which will be flushed with a save.
  void put(Sync.ObjectWrapper objectWrapper) throws IOException;

  void reject(Sync.ObjectWrapper object) throws IOException;

  void open() throws IOException;

  // Flushes and saves all mutated objects into the database and starts anew.
  void save() throws IOException;
}
