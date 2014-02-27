package io.corps.sgoc.persistence.session;

import io.corps.sgoc.persistence.session.backend.Backend;

import java.io.IOException;

public class LogStateManager {
  private final Backend backend;
  private LogState logState;

  public LogStateManager(Backend backend) {
    this.backend = backend;
  }

  public LogState getLogState() {
    return logState;
  }

  public void reset() throws IOException {
    long lastTimestamp = backend.getLastTimestamp();
    // Never let the last timestamp be 0.  This value has a special meaning when syncing.
    if(lastTimestamp <= Session.FRESH_SYNC_VERSION_NUMBER) {
      lastTimestamp = Session.FRESH_SYNC_VERSION_NUMBER + 1;
    }

    // Always be monotonically increasing.
    long writeTimestamp = backend.getCurrentTimestamp();
    if(writeTimestamp <= lastTimestamp) {
      writeTimestamp = lastTimestamp + 1;
    }

    logState = new LogState(lastTimestamp, writeTimestamp, backend.getLastLogSequence());
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