package io.corps.sgoc.session;

import io.corps.sgoc.session.exceptions.DependentPutReorderingRequest;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public interface WriteSession {
  // Adds the given object to the mutated objects cache, which will be flushed with a save.
  void put(Sync.ObjectWrapper objectWrapper) throws IOException;

  // Same as put, except that the given objectIds can be used by BeforePut to request
  // re ordering by throwing DependentPutReorderingRequest.
  void put(Sync.ObjectWrapper objectWrapper, Set<String> objectIds)
      throws IOException, DependentPutReorderingRequest;

  void reject(Sync.ObjectWrapper object) throws IOException;

  void open() throws IOException;

  // Flushes and saves all mutated objects into the database and starts anew.
  void save() throws IOException;
}
