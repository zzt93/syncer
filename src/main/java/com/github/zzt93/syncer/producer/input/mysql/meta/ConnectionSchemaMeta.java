package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.google.common.collect.Lists;
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

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final List<SchemaMeta> schemaMetas = new ArrayList<>();

  public TableMeta findTable(String database, String table) {
    // test_dev* vs test*: we will use the first that match, order is undefined
    TableMeta res = null;
    int count = 0;
    for (SchemaMeta schemaMeta : schemaMetas) {
      TableMeta tableMeta = schemaMeta.findTable(database, table);
      if (tableMeta != null) {
        count++;
      }
      if (res == null) {
        res = tableMeta;
      }
    }
    if (count > 1) {
      logger.warn("Multiple configured schema match `{}`.`{}`. Check your config", database, table);
    }
    return res;
  }

  public static class MetaDataBuilder {

    private final Logger logger = LoggerFactory.getLogger(MetaDataBuilder.class);

    private final DataSource dataSource;
    private final IdentityHashMap<ConsumerSchema, OutputSink> schemasConsumerMap;
    private final String calculatedSchemaName;

    public MetaDataBuilder(MysqlConnection connection,
        IdentityHashMap<ConsumerSchema, OutputSink> schemasConsumerMap) {
      this.schemasConsumerMap = schemasConsumerMap;
      Set<String> merged = schemasConsumerMap.keySet().stream().map(ConsumerSchema::getSchemas)
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
      IdentityHashMap<ConsumerSchema, List<SchemaMeta>> schema2Meta = build(schemasConsumerMap);
      for (Entry<ConsumerSchema, OutputSink> entry : schemasConsumerMap.entrySet()) {
        ConnectionSchemaMeta connectionSchemaMeta = new ConnectionSchemaMeta();
        if (!schema2Meta.containsKey(entry.getKey())) {
          logger.error("Fail to fetch meta info for {}", entry.getKey());
          continue;
        }
        connectionSchemaMeta.schemaMetas.addAll(schema2Meta.get(entry.getKey()));
        res.put(connectionSchemaMeta, entry.getValue());
      }
      return res;
    }

    private IdentityHashMap<ConsumerSchema, List<SchemaMeta>> build(
        IdentityHashMap<ConsumerSchema, OutputSink> schemasConsumerMap)
        throws SQLException {
      Connection connection = dataSource.getConnection();
      if (calculatedSchemaName.equals(MysqlConnection.DEFAULT_DB)) {
        // make it to get all databases
        ((JDBC4Connection) connection).setNullCatalogMeansCurrent(false);
      }
      IdentityHashMap<ConsumerSchema, List<SchemaMeta>> res;
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tableResultSet = metaData
            .getTables(null, null, "%", new String[]{"TABLE"})) {
          res = getSchemaMeta(metaData, tableResultSet, schemasConsumerMap);
        }
      } finally {
        connection.close();
      }
      return res;
    }

    private IdentityHashMap<ConsumerSchema, List<SchemaMeta>> getSchemaMeta(DatabaseMetaData metaData,
        ResultSet tableResultSet, Map<ConsumerSchema, OutputSink> schemasConsumerMap)
        throws SQLException {
      IdentityHashMap<ConsumerSchema, List<SchemaMeta>> res = new IdentityHashMap<>();
      int tableCount = 0, nowCount = 0;
      Set<ConsumerSchema> consumerSchemas = schemasConsumerMap.keySet();
      for (ConsumerSchema schemas : consumerSchemas) {
        tableCount += schemas.getSchemas().stream().mapToInt(s -> s.getTables().size()).sum();
      }

      IdentityHashMap<Schema, SchemaMeta> metaHashMap = new IdentityHashMap<>();
      while (tableCount > nowCount && tableResultSet.next()) {
        String tableSchema = tableResultSet.getString("TABLE_CAT");
        String tableName = tableResultSet.getString("TABLE_NAME");
        for (ConsumerSchema schemas : consumerSchemas) { // for each consumer registered schemas
          SchemaMeta schemaMeta = null;
          boolean newSchema = true, newTable = true;
          for (Schema aim : schemas.getSchemas()) {
            Set<String> tableRow = aim.getTableRow(tableSchema, tableName);
            if (tableRow != null) { // a consumer should only match one table at one time
              TableMeta tableMeta = new TableMeta();
              // TODO 18/1/18 may opt to get all columns then use
              setPrimaryKey(metaData, tableSchema, tableName, tableRow, tableMeta);
              setInterestedCol(metaData, tableSchema, tableName, tableRow, tableMeta);
              if (metaHashMap.containsKey(aim)) {
                newSchema = false;
              }
              schemaMeta = metaHashMap
                  .computeIfAbsent(aim, k -> new SchemaMeta(aim.getName(), aim.getNamePattern()));
              newTable = schemaMeta.addTableMeta(tableName, tableMeta) == null;
              break;
            }
          }
          if (schemaMeta != null) { // matched schema & name
            if (newTable) {
              nowCount++;
            }
            SchemaMeta finalSchemaMeta = schemaMeta;
            boolean finalCreate = newSchema;
            res.compute(schemas, (k, v) -> {
              if (v == null) {
                return Lists.newArrayList(finalSchemaMeta);
              } else {
                if (finalCreate) {
                  v.add(finalSchemaMeta);
                }
                return v;
              }
            });
          }
        }
      }
      if (tableCount < nowCount) {
        InvalidConfigException e = new InvalidConfigException();
        logger.error("Invalid schema config: actual listening {} tables, only find {}", tableCount,
            nowCount, e);
        throw e;
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
          tableMeta.addInterestedCol(columnName, ordinalPosition);
        }
      }
    }

    private void setPrimaryKey(DatabaseMetaData metaData, String tableSchema, String tableName,
        Set<String> tableRow, TableMeta tableMeta) throws SQLException {
      try (ResultSet primaryKeys = metaData.getPrimaryKeys(tableSchema, "", tableName)) {
        if (primaryKeys.next()) {
          // use `- 1` because the index of mysql column is count from 1
          int ordinalPosition = primaryKeys.getInt("KEY_SEQ") - 1;
          String columnName = primaryKeys.getString("COLUMN_NAME");
          if (!tableRow.contains(columnName)) {
            tableMeta.noPrimaryKey();
            logger.info("Not config primary key as interested column, can be accessed only in `id` but not in `record`");
          }
          tableMeta.addInterestedCol(columnName, ordinalPosition);
          tableMeta.addPrimaryKey(ordinalPosition);
        }
        if (primaryKeys.next()) {
          InvalidConfigException e = new InvalidConfigException("Not support composite primary key");
          logger.error("Not support composite primary key {}.{}", tableSchema, tableName, e);
          throw e;
        }
      }
    }

  }
}
