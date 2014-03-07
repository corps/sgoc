package io.corps.sgoc.session;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by corps@github.com on 2014/02/18.
 * Copyrighted by Zach Collins 2014
 */
public class SgocTestDatabase {
  public static Connection setup() throws SQLException, IOException {
    String migration = IOUtils.toString(
        SgocTestDatabase.class.getClassLoader().getResourceAsStream("migrations/init_sgoc.sql"), "UTF-8");

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", "root");
    Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/", connectionProperties);
    try {
      connection.prepareStatement("DROP DATABASE sgoc_test").execute();
    } catch (SQLException e) {
    }

    connection.prepareStatement("CREATE DATABASE sgoc_test").execute();
    connection.prepareStatement("USE sgoc_test").execute();

    for (String command : migration.split(";")) {
      if (command.trim().isEmpty()) continue;
      connection.prepareStatement(command).execute();
    }

    return connection;
  }
}
