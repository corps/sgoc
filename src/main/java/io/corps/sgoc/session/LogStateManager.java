package io.corps.sgoc.session;

import io.corps.sgoc.backend.BackendSession;

import java.io.IOException;

public class LogStateManager {
  private final String rootKey;
  private final BackendSession backendSession;
  private LogState logState;

  public LogStateManager(String rootKey, BackendSession backendSession) {
    this.rootKey = rootKey;
    this.backendSession = backendSession;
  }

  public LogState getLogState() {
    return logState;
  }

  public void reset() throws IOException {
    long lastTimestamp = backendSession.getLastTimestamp(rootKey);
    // Never let the last timestamp be 0.  This value has a special meaning when syncing.
    if (lastTimestamp <= Session.FRESH_SYNC_VERSION_NUMBER) {
      lastTimestamp = Session.FRESH_SYNC_VERSION_NUMBER + 1;
    }

    // Always be monotonically increasing.
    long writeTimestamp = backendSession.getCurrentTimestamp(rootKey);
    if (writeTimestamp <= lastTimestamp) {
      writeTimestamp = lastTimestamp + 1;
    }

    logState = new LogState(lastTimestamp, writeTimestamp, backendSession.getLastLogSequence(rootKey));
  }

  public class LogState {
    private final long lastTimestamp;
    private final long writeTimestamp;
    private final long lastLogSequence;

    private LogState(long lastTimestamp, long writeTimestamp, long lastLogSequence) {
      this.lastTimestamp = lastTimestamp;
      this.writeTimestamp = writeTimestamp;
      this.lastLogSequence = lastLogSequence;
    }

    @Override
    public int hashCode() {
      int result = (int) (lastTimestamp ^ (lastTimestamp >>> 32));
      result = 31 * result + (int) (writeTimestamp ^ (writeTimestamp >>> 32));
      result = 31 * result + (int) (lastLogSequence ^ (lastLogSequence >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LogState logState = (LogState) o;

      if (lastLogSequence != logState.lastLogSequence) return false;
      if (lastTimestamp != logState.lastTimestamp) return false;
      if (writeTimestamp != logState.writeTimestamp) return false;

      return true;
    }

    public long getLastTimestamp() {
      return lastTimestamp;
    }

    public long getWriteTimestamp() {
      return writeTimestamp;
    }

    public long getLastLogSequence() {
      return lastLogSequence;
    }
  }
}