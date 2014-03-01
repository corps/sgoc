package io.corps.sgoc.session.factory;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Throwables;
import io.corps.sgoc.backend.mysql.MysqlBackendSession;
import io.corps.sgoc.session.Session;
import io.corps.sgoc.session.config.SessionConfig;
import io.corps.sgoc.session.exceptions.WriteContentionException;
import org.jooq.ConnectionProvider;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by corps@github.com on 2014/03/03.
 * Copyrighted by Zach Collins 2014
 */
public class SgocSessionFactory implements SessionFactory {
  private final SessionConfig config;
  private final ConnectionProvider connectionProvider;

  public SgocSessionFactory(SessionConfig config, ConnectionProvider connectionProvider) {
    this.config = config;
    this.connectionProvider = connectionProvider;
  }

  @Override
  public <T> T doWork(String rootKey, final ReadWriteSessionWork<T> work) throws IOException {
    Connection connection = connectionProvider.acquire();
    try {
      try (final Session session = new Session(rootKey,
          new MysqlBackendSession(connection, config.getExtensionRegistry()), config)) {
        session.open();
        return RetryerBuilder.<T>newBuilder()
            .retryIfExceptionOfType(WriteContentionException.class)
            .withWaitStrategy(WaitStrategies.exponentialWait(500, TimeUnit.MILLISECONDS))
            .withStopStrategy(StopStrategies.stopAfterAttempt(config.getNumRetries()))
            .build().wrap(new Callable<T>() {
              @Override
              public T call() throws Exception {
                return work.doWork(session);
              }
            }).call();
      } catch (ExecutionException | RetryException e) {
        throw Throwables.propagate(e.getCause());
      }
    } finally {
      connectionProvider.release(connection);
    }
  }

  @Override
  public <T> T doReadOnlyWork(String rootKey, ReadOnlySessionWork<T> work) throws IOException {
    Connection connection = connectionProvider.acquire();
    try {
      try (final Session session = new Session(rootKey,
          new MysqlBackendSession(connection, config.getExtensionRegistry()), config)) {
        session.open();
        return work.doWork(session);
      }
    } finally {
      connectionProvider.release(connection);
    }
  }
}
