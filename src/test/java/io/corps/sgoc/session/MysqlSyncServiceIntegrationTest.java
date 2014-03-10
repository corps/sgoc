package io.corps.sgoc.session;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.corps.sgoc.session.config.SessionConfig;
import io.corps.sgoc.session.factory.SessionFactory;
import io.corps.sgoc.session.factory.SgocSessionFactory;
import io.corps.sgoc.sync.SgocSyncService;
import io.corps.sgoc.sync.SyncService;
import org.jooq.impl.DataSourceConnectionProvider;

/**
 * Created by corps@github.com on 2014/03/03.
 * Copyrighted by Zach Collins 2014
 */
public class MysqlSyncServiceIntegrationTest extends AbstractSyncServiceIntegrationTest {
  private ComboPooledDataSource cpds;

  @Override
  protected SessionFactory getSessionFactory(SessionConfig config) throws Exception {
    cpds = new ComboPooledDataSource();
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/sgoc_test");
    cpds.setUser("root");
    cpds.setAutoCommitOnClose(false);
    cpds.setMaxPoolSize(4);
    cpds.setMinPoolSize(2);
    cpds.setPassword("");

    return new SgocSessionFactory(config, new DataSourceConnectionProvider(cpds));
  }

  @Override
  protected SyncService getSyncService(SessionConfig config, SessionFactory sessionFactory) throws Exception {
    return new SgocSyncService(sessionFactory, config.getEntitySchema());
  }

  @Override
  protected void close() throws Exception {
    cpds.close();
  }
}
