package com.github.zzt93.syncer.common;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.input.Schema;
import com.mysql.jdbc.JDBC4Connection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * @author zzt
 */
public class SchemaMeta {


  private final HashMap<String, TableMeta> tableMetas = new HashMap<>();
  private final Pattern schemaPattern;
  private final String schema;

  private SchemaMeta(String name, Pattern namePattern) {
    this.schemaPattern = namePattern;
    this.schema = name;
  }

  private void addTableMeta(String name, TableMeta tableMeta) {
    tableMetas.put(name, tableMeta);
  }

  private TableMeta findTable(String database, String table) {
    // schema match?
    if (schema.equals(database) ||
        (schemaPattern != null && schemaPattern.matcher(database).find())) {
      // table name match?
      return tableMetas.getOrDefault(table, null);
    }
    return null;
  }

  public boolean filterRow(RowEvent rowEvent) {
    TableMapEventData data = rowEvent.getTableMap();
    TableMeta table = findTable(data.getDatabase(), data.getTable());
    return table != null && rowEvent.filterData(table.getIndex());
  }

  public static class MetaDataBuilder {

    private final Logger logger = LoggerFactory.getLogger(MetaDataBuilder.class);

    private final Schema interested;
    private final DataSource dataSource;

    public MetaDataBuilder(MysqlConnection connection, Schema schema) {
      this.interested = schema;
      dataSource = new DriverManagerDataSource(
          connection.toConnectionUrl(schema.getConnectionName()), connection.getUser(),
          connection.getPassword());
    }

    public SchemaMeta build() throws SQLException {
      SchemaMeta res = new SchemaMeta(interested.getName(), interested.getNamePattern());
      Connection connection = dataSource.getConnection();
      // make it to get all data bases for test
      ((JDBC4Connection) connection).setNullCatalogMeansCurrent(false);
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tableResultSet = metaData
            .getTables(null, null, "%", new String[]{"TABLE"})) {
          while (!finished(res) && tableResultSet.next()) {
            String tableSchema = tableResultSet.getString("TABLE_CAT");
            String tableName = tableResultSet.getString("TABLE_NAME");
            Set<String> tableRow = interested.getTableRow(tableSchema, tableName);
            if (tableRow == null) {
              continue;
            }
            TableMeta tableMeta = new TableMeta();
            try (ResultSet columnResultSet = metaData.getColumns(null, "public", tableName, null)) {
              while (columnResultSet.next()) {
                String columnName = columnResultSet.getString("COLUMN_NAME");
                if (!tableRow.contains(columnName)) {
                  continue;
                }
                int ordinalPosition = columnResultSet.getInt("ORDINAL_POSITION");
                tableMeta.addNameIndex(ordinalPosition);
              }
            }
            res.addTableMeta(tableName, tableMeta);
          }
        }
      } finally {
        connection.close();
      }
      return res;
    }

    private boolean finished(SchemaMeta res) {
      boolean b = res.tableMetas.size() == interested.getTables().size();
      if (b) {
        logger.info("Finished the meta data fetch {}", res.tableMetas);
      }
      return b;
    }
  }
}
