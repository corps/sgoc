package io.corps.sgoc.persistence.session.backend.mysql;

import com.google.common.collect.Lists;
import io.corps.sgoc.sync.Sync;
import io.corps.sgoc.persistence.session.SgocTestDatabase;
import org.jooq.conf.Settings;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

/**
 * Created by corps@github.com on 2014/02/17.
 * Copyrighted by Zach Collins 2014
 */
public class MysqlBackendTest {
  private Connection connection;
  private MysqlBackend backend;

  @Before
  public void setUp() throws Exception {
    connection = SgocTestDatabase.setup();
    backend = new MysqlBackend(connection, new Settings());
  }

  @Test
  public void testLookup() throws Exception {
    backend.lookup("abc", Lists.<Sync.ObjectId>newArrayList());
  }
}
