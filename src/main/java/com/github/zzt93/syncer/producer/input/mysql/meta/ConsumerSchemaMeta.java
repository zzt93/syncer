package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.google.common.collect.Lists;
import com.mysql.jdbc.JDBC4Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * All schema metas {@link SchemaMeta} that a DB has.
 * A DB is identified by connection identifier (host + port).
 *
 * @see SchemaMeta
 * @see com.github.zzt93.syncer.config.pipeline.common.Connection#connectionIdentifier()
 * @author zzt
 */
public class ConsumerSchemaMeta {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final List<SchemaMeta> schemaMetas = new ArrayList<>();
  private final String id;

  private ConsumerSchemaMeta(String id) {
    this.id = id;
  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConsumerSchemaMeta that = (ConsumerSchemaMeta) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public static class MetaDataBuilder {

    private final Logger logger = LoggerFactory.getLogger(MetaDataBuilder.class);

    private final DataSource dataSource;
    private final HashMap<Consumer, ProducerSink> consumerSink;
    private final String calculatedSchemaName;

    public MetaDataBuilder(MysqlConnection connection,
                           HashMap<Consumer, ProducerSink> consumerSink) {
      this.consumerSink = consumerSink;
      Set<String> merged = consumerSink.keySet().stream().map(Consumer::getSchemas)
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

    public HashMap<ConsumerSchemaMeta, ProducerSink> build() throws SQLException {
      HashMap<ConsumerSchemaMeta, ProducerSink> res = new HashMap<>();
      HashMap<Consumer, List<SchemaMeta>> def2data = build(consumerSink);
      for (Entry<Consumer, ProducerSink> entry : consumerSink.entrySet()) {
        Consumer consumer = entry.getKey();
        if (!def2data.containsKey(consumer)) {
          logger.error("Fail to fetch meta info for {}", consumer);
          continue;
        }
        ConsumerSchemaMeta consumerSchemaMeta = new ConsumerSchemaMeta(consumer.getId());
        consumerSchemaMeta.schemaMetas.addAll(def2data.get(consumer));
        res.put(consumerSchemaMeta, entry.getValue());
      }
      return res;
    }

    private HashMap<Consumer, List<SchemaMeta>> build(HashMap<Consumer, ProducerSink> consumerSink)
        throws SQLException {
      Connection connection = dataSource.getConnection();
      if (calculatedSchemaName.equals(MysqlConnection.DEFAULT_DB)) {
        // make it to get all databases
        ((JDBC4Connection) connection).setNullCatalogMeansCurrent(false);
      }
      HashMap<Consumer, List<SchemaMeta>> res;
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tableResultSet = metaData
            .getTables(null, null, "%", new String[]{"TABLE"})) {
          res = getSchemaMeta(metaData, tableResultSet, consumerSink.keySet());
        }
      } finally {
        connection.close();
      }
      return res;
    }

    private HashMap<Consumer, List<SchemaMeta>> getSchemaMeta(DatabaseMetaData metaData,
                                                              ResultSet tableResultSet,
                                                              Set<Consumer> consumers)
        throws SQLException {
      HashMap<Consumer, List<SchemaMeta>> res = new HashMap<>();
      int tableCount = 0, nowCount = 0;
      for (Consumer consumer : consumers) {
        tableCount += consumer.getSchemas().stream().mapToInt(s -> s.getTables().size()).sum();
      }

      HashMap<Schema, SchemaMeta> metaHashMap = new HashMap<>();
      while (tableCount > nowCount && tableResultSet.next()) {
        String tableSchema = tableResultSet.getString("TABLE_CAT");
        String tableName = tableResultSet.getString("TABLE_NAME");
        for (Consumer consumer : consumers) {
          SchemaMeta schemaMeta = null;
          boolean newSchema = true, newTable = true;
          for (Schema aim : consumer.getSchemas()) {
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
            res.compute(consumer, (k, v) -> {
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
