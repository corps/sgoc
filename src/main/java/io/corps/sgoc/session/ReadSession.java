package io.corps.sgoc.session;

import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.sync.Sync;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/16.
 * Copyrighted by Zach Collins 2014
 */
public interface ReadSession extends Closeable {
  // Fetches the given object ids into the session cache in an efficient manner.
  void prefetchObjects(Collection<String> objectIds) throws IOException;

  void prefetchIndexLookups(Iterable<IndexLookup> indexLookups) throws IOException;

  void open() throws IOException;

  // Fetches a single object by its object id, using its session cached value if it's available.
  Sync.ObjectWrapper get(String id) throws IOException;

  Set<String> indexLookup(IndexLookup indexLookup) throws IOException;

  Sync.GetResponse changesSince(long timestamp) throws IOException;
}
