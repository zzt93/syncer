package com.github.zzt93.syncer.common;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.mysql.jdbc.JDBC4Connection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Connection based schema meta, share same connection to a single connection to DB
 *
 * @author zzt
 */
public class ConnectionSchemaMeta {

  private List<SchemaMeta> schemaMetas = new ArrayList<>();

  public TableMeta findTable(String database, String table) {
    for (SchemaMeta schemaMeta : schemaMetas) {
      TableMeta tableMeta = schemaMeta.findTable(database, table);
      if (tableMeta != null) {
        return tableMeta;
      }
    }
    return null;
  }

  public static class MetaDataBuilder {

    private final Logger logger = LoggerFactory.getLogger(MetaDataBuilder.class);

    private final List<Schema> interested;
    private final DataSource dataSource;

    public MetaDataBuilder(MysqlConnection connection, List<Schema> schema) {
      this.interested = schema;
      String calculatedSchemaName = getSchemaName(schema);
      dataSource = new DriverManagerDataSource(connection.toConnectionUrl(calculatedSchemaName),
          connection.getUser(), connection.getPassword());
    }

    private String getSchemaName(List<Schema> schema) {
      if (schema.size() == 1 && !schema.get(0).hasNamePattern()) {
        return schema.get(0).getConnectionName();
      }
      return MysqlConnection.DEFAULT_DB;
    }

    public ConnectionSchemaMeta build() throws SQLException {
      ConnectionSchemaMeta res = new ConnectionSchemaMeta();
      for (Schema schema : interested) {
        SchemaMeta meta = new SchemaMeta(schema.getName(), schema.getNamePattern());
        build(meta, schema);
        res.schemaMetas.add(meta);
      }
      return res;
    }

    private void build(SchemaMeta schemaMeta, Schema aim) throws SQLException {
      Connection connection = dataSource.getConnection();
      // make it to get all databases
      if (aim.hasNamePattern()) {
        ((JDBC4Connection) connection).setNullCatalogMeansCurrent(false);
      }
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        // TODO 17/12/29 test schemaPattern
        try (ResultSet tableResultSet = metaData
            .getTables(null, null, "%", new String[]{"TABLE"})) {
          while (!finished(schemaMeta, aim) && tableResultSet.next()) {
            String tableSchema = tableResultSet.getString("TABLE_CAT");
            String tableName = tableResultSet.getString("TABLE_NAME");
            Set<String> tableRow = aim.getTableRow(tableSchema, tableName);
            if (tableRow == null) {
              continue;
            }
            TableMeta tableMeta = new TableMeta();
            try (ResultSet primaryKeys = metaData.getPrimaryKeys(tableSchema, "", tableName)) {
              if (primaryKeys.next()) {
                // use - 1 because the index of mysql column is count from 1
                int ordinalPosition = primaryKeys.getInt("KEY_SEQ") - 1;
                String columnName = primaryKeys.getString("COLUMN_NAME");
                if (!tableRow.contains(columnName)) {
                  logger.warn("Not config primary key as interested column, adding it anyway");
                }
                tableMeta.addNameIndex(columnName, ordinalPosition);
                tableMeta.addPrimaryKey(ordinalPosition);
              }
              if (primaryKeys.next()) {
                InvalidConfigException e = new InvalidConfigException(
                    "Not support composite primary key");
                logger.error("Not support composite primary key {}.{}", tableSchema, tableName, e);
                throw e;
              }
            }
            try (ResultSet columnResultSet = metaData.getColumns(tableSchema, "public", tableName, null)) {
              while (columnResultSet.next()) {
                String columnName = columnResultSet.getString("COLUMN_NAME");
                if (!tableRow.contains(columnName)) {
                  continue;
                }
                // use - 1 because the index of mysql column is count from 1
                int ordinalPosition = columnResultSet.getInt("ORDINAL_POSITION") - 1;
                tableMeta.addNameIndex(columnName, ordinalPosition);
              }
            }
            schemaMeta.addTableMeta(tableName, tableMeta);
          }
        }
      } finally {
        connection.close();
      }
    }

    private boolean finished(SchemaMeta res, Schema aim) {
      boolean b = res.size() == aim.getTables().size();
      if (b) {
        logger.info("Finished the meta data fetch {}", res);
      }
      return b;
    }
  }
}
