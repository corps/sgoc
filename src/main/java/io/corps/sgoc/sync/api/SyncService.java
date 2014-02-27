package io.corps.sgoc.sync.api;

import io.corps.sgoc.sync.Sync;

/**
 * Created by corps@github.com on 2014/02/15.
 * Copyrighted by Zach Collins 2014
 */
public interface SyncService {
  public Sync.GetResponse get(Sync.GetRequest request);

  public Sync.PutResponse put(Sync.PutResponse response);
}
