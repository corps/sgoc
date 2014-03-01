package io.corps.sgoc.backend;

import com.google.common.collect.Multimap;
import io.corps.sgoc.schema.IndexLookup;
import io.corps.sgoc.session.LogStateManager;
import io.corps.sgoc.sync.Sync;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public interface BackendSession {
  void rollback() throws IOException;

  List<Sync.ObjectWrapper> lookup(String rootKey, Collection<String> idsToLookup) throws IOException;

  Multimap<IndexLookup, String> lookupIndexes(String rootKey, Collection<IndexLookup> lookups) throws IOException;

  long getLastTimestamp(String rootKey) throws IOException;

  long getCurrentTimestamp(String rootKey) throws IOException;

  long getLastLogSequence(String rootKey) throws IOException;

  void writeLogEntry(String rootKey, LogStateManager.LogState logState) throws IOException;

  public void writeObjects(String rootKey, Collection<Sync.ObjectWrapper> values, LogStateManager.LogState logState)
      throws IOException;

  void removeIndexEntries(String rootKey, Collection<Map.Entry<IndexLookup, String>> removedIndexEntries, LogStateManager.LogState logState) throws IOException;

  void addIndexEntries(String rootKey, Collection<Map.Entry<IndexLookup, String>> entries, LogStateManager.LogState logState) throws IOException;

  void commit() throws IOException;

  List<Sync.ObjectWrapper> changesSince(String rootKey, long timestamp) throws IOException;
}
