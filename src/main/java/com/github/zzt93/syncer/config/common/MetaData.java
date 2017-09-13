package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.input.Schema;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * @author zzt
 */
public class MetaData {


  public static class MetaDataBuilder {

    private DataSource dataSource;

    public MetaDataBuilder(MysqlConnection connection, Schema schema) {
      dataSource = new DriverManagerDataSource(
          connection.toConnectionUrl(schema.getConnectionName()), connection.getUser(),
          connection.getPassword());
    }

    public MetaData build() throws SQLException {
      MetaData res = new MetaData();
      Connection connection = dataSource.getConnection();
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet tableResultSet = metaData
          .getTables(null, "public", null, new String[]{"TABLE"})) {
        while (tableResultSet.next()) {
          String tableName = tableResultSet.getString("TABLE_NAME");
          try (ResultSet columnResultSet = metaData.getColumns(null, "public", tableName, null)) {
            while (columnResultSet.next()) {
              String columnName = columnResultSet.getString("COLUMN_NAME");
              System.out.println(columnName);
            }
          }
        }
      }
      return res;
    }
  }
}
