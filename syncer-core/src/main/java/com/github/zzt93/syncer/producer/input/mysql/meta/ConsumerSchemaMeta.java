package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.consumer.input.Entity;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.output.channel.elastic.ElasticsearchChannel;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.google.common.collect.Lists;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.JDBC4Connection;
import com.zaxxer.hikari.util.DriverDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @author zzt
 * @see SchemaMeta
 * @see com.github.zzt93.syncer.config.common.Connection#connectionIdentifier()
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
      logger.error("Multiple configured schema match `{}`.`{}`. Check your config", database, table);
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

  @Override
  public String toString() {
    return "ConsumerSchemaMeta{" +
        "schemaMetas=" + schemaMetas +
        ", id='" + id + '\'' +
        '}';
  }

  public static class MetaDataBuilder {

    static final int TIMEOUT = 10;
    private final Logger logger = LoggerFactory.getLogger(MetaDataBuilder.class);

    private final DataSource dataSource;
    private final HashMap<Consumer, ProducerSink> consumerSink;
    private final String calculatedSchemaName;
    private final String jdbcUrl;

    public MetaDataBuilder(MysqlConnection connection,
                           HashMap<Consumer, ProducerSink> consumerSink) throws SQLException {
      this.consumerSink = consumerSink;
      Set<String> merged = consumerSink.keySet().stream().map(Consumer::getRepos)
          .flatMap(Set::stream).map(Repo::getConnectionName).collect(Collectors.toSet());
      calculatedSchemaName = getSchemaName(merged);
      jdbcUrl = connection.toConnectionUrl(calculatedSchemaName);
      dataSource = new DriverDataSource(jdbcUrl,
          Driver.class.getName(), new Properties(),
          connection.getUser(), connection.getPassword());
      dataSource.setLoginTimeout(TIMEOUT);
    }

    private String getSchemaName(Set<String> schema) {
      if (schema.size() == 1) {
        return schema.iterator().next();
      }
      return MysqlConnection.DEFAULT_DB;
    }

    public HashMap<ConsumerSchemaMeta, ProducerSink> build() throws SQLException {
      HashMap<ConsumerSchemaMeta, ProducerSink> res = new HashMap<>();
      HashMap<Consumer, List<SchemaMeta>> def2data = build(consumerSink.keySet());
      for (Entry<Consumer, ProducerSink> entry : consumerSink.entrySet()) {
        Consumer consumer = entry.getKey();
        List<SchemaMeta> metas = def2data.get(consumer);
        if (metas == null ||
            consumer.getRepos().size() != metas.size()) {
          logger.error("Fail to fetch meta info for {}", diff(consumer.getRepos(), metas));
          throw new InvalidConfigException("Fail to fetch meta info");
        }
        ConsumerSchemaMeta consumerSchemaMeta = new ConsumerSchemaMeta(consumer.getId());
        consumerSchemaMeta.schemaMetas.addAll(metas);
        res.put(consumerSchemaMeta, entry.getValue());
      }
      return res;
    }

    private Set<Repo> diff(Set<Repo> repos, List<SchemaMeta> metas) {
      if (metas != null) {
        for (SchemaMeta meta : metas) {
          Repo o = new Repo();
          o.setName(meta.getSchema());
          repos.remove(o);
        }
      }
      return repos;
    }

    private HashMap<Consumer, List<SchemaMeta>> build(Set<Consumer> consumers)
        throws SQLException {
      logger.info("Getting connection[{}], timeout in {}s", jdbcUrl,TIMEOUT);
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
          res = getSchemaMeta(metaData, tableResultSet, consumers);
        }
      } finally {
        connection.close();
      }
      logger.info("Fetched {} for {}", res, consumers);
      return res;
    }

    private HashMap<Consumer, List<SchemaMeta>> getSchemaMeta(DatabaseMetaData metaData,
                                                              ResultSet tableResultSet,
                                                              Set<Consumer> consumers)
        throws SQLException {
      HashMap<Consumer, List<SchemaMeta>> res = new HashMap<>();
      int tableCount = 0, nowCount = 0;
      for (Consumer consumer : consumers) {
        tableCount += consumer.getRepos().stream().mapToInt(s -> s.getEntities().size()).sum();
      }

      // It is a mapping for each consumer (because diff consumer may have diff interested col, can't share),
      // so use IdentityHashMap (Map<Consumer, Map> is the same)
      IdentityHashMap<Repo, SchemaMeta> metaOfEachConsumer = new IdentityHashMap<>();
      while (tableCount > nowCount && tableResultSet.next()) { // for each table
        String tableSchema = tableResultSet.getString("TABLE_CAT");
        String tableName = tableResultSet.getString("TABLE_NAME");
        for (Consumer consumer : consumers) { // if any consumer interested in
          Repo aim = consumer.matchedSchema(tableSchema, tableName);
          if (aim != null) {
            SchemaMeta schemaMeta = metaOfEachConsumer.computeIfAbsent(aim, k -> {
              SchemaMeta tmp = new SchemaMeta(aim.getName(), aim.getNamePattern());
              // add meta to consumer map if new schema
              res.computeIfAbsent(consumer, key -> Lists.newLinkedList()).add(tmp);
              return tmp;
            });
            // remove for drds situation re-enter
            Set<String> tableRow = aim.removeTableRow(tableSchema, tableName);
            TableMeta tableMeta = new TableMeta();
            // TODO 18/1/18 may opt to get all columns then use
            String primaryKeyName = getPrimaryKey(metaData, tableSchema, tableName, tableRow, tableMeta);
            setInterestedCol(metaData, tableSchema, tableName, tableRow, tableMeta, primaryKeyName);
            schemaMeta.addTableMeta(tableName, tableMeta);
            nowCount++;
          }
        }
      }
      if (tableCount > nowCount) {
        logger.error("Invalid schema config: want {} but not found", diff(metaOfEachConsumer));
        throw new InvalidConfigException("Invalid schema config");
      }
      return res;
    }

    private List<Entity> diff(Map<Repo, SchemaMeta> metaOfEachConsumer) {
      List<Entity> res = new ArrayList<>();
      metaOfEachConsumer.forEach((k, v) -> {
        List<Entity> entities = k.getEntities();
        if (entities.size() != v.size()) {
          for (Entity entity : entities) {
            if (v.findTable(k.getName(), entity.getName()) == null) {
              res.add(entity);
            }
          }
        }
      });
      return res;
    }

    private void setInterestedCol(DatabaseMetaData metaData, String tableSchema, String tableName,
                                  Set<String> tableRow, TableMeta tableMeta, String primaryKeyName) throws SQLException {
      try (ResultSet columnResultSet = metaData
          .getColumns(tableSchema, "public", tableName, null)) {
        while (columnResultSet.next()) {
          String columnName = columnResultSet.getString("COLUMN_NAME");
          // use - 1 because the index of mysql column is count from 1
          int ordinalPosition = columnResultSet.getInt("ORDINAL_POSITION") - 1;
          if (primaryKeyName.equals(columnName)) {
            tableMeta.addPrimaryKey(columnName, ordinalPosition);
          }
          if (tableRow.contains(columnName)) {
            tableMeta.addInterestedCol(columnName, ordinalPosition);
          }
        }
      }
    }

    private String getPrimaryKey(DatabaseMetaData metaData, String tableSchema, String tableName,
                                 Set<String> tableRow, TableMeta tableMeta) throws SQLException {
      try (ResultSet primaryKeys = metaData.getPrimaryKeys(tableSchema, "", tableName)) {
        if (primaryKeys.next()) {
          String columnName = primaryKeys.getString("COLUMN_NAME");
          if (!tableRow.contains(columnName)) {
            tableMeta.noPrimaryKey();
            logger.info("Not config primary key as interested column in [{}.{}], can be accessed only in `id` but not in `field`", tableSchema, tableName);
          }
          if (primaryKeys.next()) {
            logger.error("Not support multiple/composite primary key {}.{}", tableSchema, tableName);
            throw new InvalidConfigException("Not support composite primary key");
          }
          return columnName;
        } else {
          logger.error("Fail to fetch primary key or no primary key for {}.{}", tableSchema, tableName);
          throw new InvalidConfigException("Not support table without primary key");
        }
      }
    }

  }
}
