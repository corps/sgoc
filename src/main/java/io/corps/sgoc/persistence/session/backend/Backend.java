package io.corps.sgoc.persistence.session.backend;

import com.google.common.collect.Multimap;
import io.corps.sgoc.persistence.session.LogStateManager;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.sync.Sync;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public interface Backend extends Closeable {
  List<Sync.ObjectWrapper> lookup(String rootKey, List<Sync.ObjectId> idsToLookup) throws IOException;

  Multimap<IndexLookup, Sync.ObjectId> lookupIndexes(String rootKey, List<IndexLookup> lookups) throws IOException;

  long getLastTimestamp() throws IOException;

  long getCurrentTimestamp() throws IOException;

  long getLastLogSequence() throws IOException;

  void writeLogEntry(LogStateManager.LogState logState) throws IOException;

  void writeObjects(Collection<Sync.ObjectWrapper> values) throws IOException;

  void removeIndexEntries(Collection<Map.Entry<IndexLookup, Sync.ObjectId>> removedIndexEntries) throws IOException;

  void addIndexEntries(Collection<Map.Entry<IndexLookup,Sync.ObjectId>> entries) throws IOException;

  void commit() throws IOException;

  List<Sync.ObjectWrapper> changesSince(long timestamp) throws IOException;
}
