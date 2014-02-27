package io.corps.sgoc.persistence.session;

import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.sync.Sync;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Created by corps@github.com on 2014/02/16.
 * Copyrighted by Zach Collins 2014
 */
public interface ReadSession extends Closeable {
  // Fetches the given object ids into the session cache in an efficient manner.
  void prefetchObjects(Collection<Sync.ObjectId> objectIds) throws IOException;

  void prefetchIndexLookups(Iterable<IndexLookup> indexLookups) throws IOException;

  void open() throws IOException;

  // Fetches a single object by its object id, using its session cached value if it's available.
  Sync.ObjectWrapper get(Sync.ObjectId id) throws IOException;

  Set<Sync.ObjectId> indexLookup(IndexLookup indexLookup) throws IOException;

  Sync.GetResponse changesSince(long timestamp) throws IOException;
}
