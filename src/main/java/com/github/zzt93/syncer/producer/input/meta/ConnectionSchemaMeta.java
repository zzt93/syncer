package com.github.zzt93.syncer.producer.input.meta;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.mysql.jdbc.JDBC4Connection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
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

  private final List<SchemaMeta> schemaMetas = new ArrayList<>();

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

    private final DataSource dataSource;
    private final IdentityHashMap<Set<Schema>, OutputSink> schema;
    private final String calculatedSchemaName;

    public MetaDataBuilder(MysqlConnection connection, IdentityHashMap<Set<Schema>, OutputSink> schema) {
      this.schema = schema;
      Set<String> merged = schema.keySet().stream()
          .flatMap(Set::stream).map(Schema::getConnectionName).collect(Collectors.toSet());
      calculatedSchemaName = getSchemaName(merged);
      dataSource = new DriverManagerDataSource(connection.toConnectionUrl(calculatedSchemaName),
          connection.getUser(), connection.getPassword());
    }

    private String getSchemaName(Set<String> schema) {
      if (schema.size() == 1) {
        return schema.iterator().next();
      }
      return MysqlConnection.DEFAULT_DB;
    }

    public IdentityHashMap<ConnectionSchemaMeta, OutputSink> build() throws SQLException {
      IdentityHashMap<ConnectionSchemaMeta, OutputSink> res = new IdentityHashMap<>();
      IdentityHashMap<Set<Schema>, List<SchemaMeta>> schema2Meta = build(schema);
      for (Entry<Set<Schema>, OutputSink> entry : schema.entrySet()) {
        ConnectionSchemaMeta connectionSchemaMeta = new ConnectionSchemaMeta();
        connectionSchemaMeta.schemaMetas.addAll(schema2Meta.get(entry.getKey()));
        res.put(connectionSchemaMeta, entry.getValue());
      }
      return res;
    }

    private IdentityHashMap<Set<Schema>, List<SchemaMeta>> build(Map<Set<Schema>, OutputSink> aims)
        throws SQLException {
      Connection connection = dataSource.getConnection();
      if (calculatedSchemaName.equals(MysqlConnection.DEFAULT_DB)) {
        // make it to get all databases
        ((JDBC4Connection) connection).setNullCatalogMeansCurrent(false);
      }
      IdentityHashMap<Set<Schema>, List<SchemaMeta>> res;
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tableResultSet = metaData
            .getTables(null, null, "%", new String[]{"TABLE"})) {
          res = getSchemaMeta(metaData, tableResultSet, aims);
        }
      } finally {
        connection.close();
      }
      return res;
    }

    private IdentityHashMap<Set<Schema>, List<SchemaMeta>> getSchemaMeta(DatabaseMetaData metaData,
        ResultSet tableResultSet, Map<Set<Schema>, OutputSink> aims)
        throws SQLException {
      IdentityHashMap<Set<Schema>, List<SchemaMeta>> res = new IdentityHashMap<>();
      int count = 0, nowCount = 0;
      Set<Set<Schema>> sets = aims.keySet();
      for (Set<Schema> aim : sets) {
        count += aim.size();
      }

      IdentityHashMap<Schema, SchemaMeta> metaHashMap = new IdentityHashMap<>();
      while (count > nowCount && tableResultSet.next()) {
        String tableSchema = tableResultSet.getString("TABLE_CAT");
        String tableName = tableResultSet.getString("TABLE_NAME");
        for (Set<Schema> set : sets) {
          List<SchemaMeta> schemaMetas = new ArrayList<>(set.size());
          for (Schema aim : set) {
            Set<String> tableRow = aim.getTableRow(tableSchema, tableName);
            if (tableRow == null) {
              continue;
            }
            TableMeta tableMeta = new TableMeta();
            // TODO 18/1/18 may opt to get all columns then use
            setPrimaryKey(metaData, tableSchema, tableName, tableRow, tableMeta);
            setInterestedCol(metaData, tableSchema, tableName, tableRow, tableMeta);
            // TODO 18/1/18 menkor_dev* vs menkor*
            boolean contain = metaHashMap.containsKey(aim);
            SchemaMeta schemaMeta = metaHashMap.computeIfAbsent(aim, k -> new SchemaMeta(aim.getName(), aim.getNamePattern()));
            schemaMeta.addTableMeta(tableName, tableMeta);
            if (!contain) {
              schemaMetas.add(schemaMeta);
            }
          }
          nowCount += schemaMetas.size();
          res.compute(set, (k, v) -> {
            if (v == null) {
              return schemaMetas;
            } else {
              v.addAll(schemaMetas);
              return v;
            }
          });
        }
      }
      return res;
    }

    private void setInterestedCol(DatabaseMetaData metaData, String tableSchema, String tableName,
        Set<String> tableRow, TableMeta tableMeta) throws SQLException {
      try (ResultSet columnResultSet = metaData
          .getColumns(tableSchema, "public", tableName, null)) {
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
    }

    private void setPrimaryKey(DatabaseMetaData metaData, String tableSchema, String tableName,
        Set<String> tableRow, TableMeta tableMeta) throws SQLException {
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
          logger
              .error("Not support composite primary key {}.{}", tableSchema, tableName, e);
          throw e;
        }
      }
    }

  }
}
