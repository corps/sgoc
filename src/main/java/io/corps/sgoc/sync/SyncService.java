package io.corps.sgoc.sync;

import java.io.IOException;

/**
 * Created by corps@github.com on 2014/02/15.
 * Copyrighted by Zach Collins 2014
 */
public interface SyncService {
  public Sync.GetResponse get(Sync.GetRequest request) throws IOException;

  public Sync.PutResponse put(Sync.PutRequest request) throws IOException;
}
