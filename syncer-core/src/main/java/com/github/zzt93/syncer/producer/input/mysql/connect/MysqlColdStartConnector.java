package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.mysql.cj.jdbc.Driver;
import com.zaxxer.hikari.util.DriverDataSource;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author zzt
 */
@Slf4j
public class MysqlColdStartConnector implements MasterConnector {
  private final JdbcTemplate jdbcTemplate;
  private final List<ColdStart> colds;
  private final MysqlConnection connection;
  private DataId nowDataId;

  public MysqlColdStartConnector(MysqlConnection connection, List<ColdStart> colds) {
    this.connection = connection;
    jdbcTemplate = new JdbcTemplate(this.connection.dataSource());
    this.colds = colds;
    nowDataId = init(jdbcTemplate);
  }

  private DataId init(JdbcTemplate jdbcTemplate) {
    List<Map> binaryLogs = jdbcTemplate.query("show binary logs", new BeanPropertyRowMapper<>(Map.class));
//    BinlogDataId dataId = DataId.fromEvent(events, binlogInfo.get().getBinlogFilename());
    return null;
  }

  @Override
  public void close() {

  }

  @SneakyThrows
  @Override
  public void loop() {
    long start = System.currentTimeMillis();
    log.info("[Cold start] at {}", start);
    for (ColdStart coldStart : colds) {
      if (!coldStart.isDrds()) {
        String repo = coldStart.getRepo();
        coldStart(coldStart.getProducerSink(), coldStart, repo);
      } else {
        String jdbcUrl = connection.toConnectionUrl(null);
        DataSource dataSource = new DriverDataSource(jdbcUrl, Driver.class.getName(), new Properties(),
            connection.getUser(), connection.getPassword());
        try (Connection dataSourceConnection = dataSource.getConnection()) {
          DatabaseMetaData metaData = dataSourceConnection.getMetaData();
          try (ResultSet tableResultSet = metaData
              .getTables(null, coldStart.getRepo(), null, new String[]{"TABLE"})) {
            while (tableResultSet.next()) {
              String tableSchema = tableResultSet.getString("TABLE_CAT");
              coldStart(coldStart.getProducerSink(), coldStart, tableSchema);
            }
          }
        }
      }
    }
    log.info("[Cold done] take {}", System.currentTimeMillis() - start);
  }

  private void coldStart(ProducerSink producerSink, ColdStart coldStart, String repo) {
    String entity = coldStart.getEntity(), pkName = coldStart.getPkName();
    int pageSize = coldStart.getPageSize();
    producerSink.markColdStart(repo, entity);

    String sql = String.format("select min(%s) as minId, max(%s) as maxId, count(1) as count from `%s`.`%s`", pkName, pkName, repo, entity);
    ColdStartStatistic stat = jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<>(ColdStartStatistic.class));
    if (stat == null) {
      throw new InvalidConfigException(coldStart.toString());
    }

    log.info("[Cold start] [{}.{}] count({}) {} [{}, {}] pageSize={}", repo, entity, stat.getCount(), pkName, stat.getMinId(), stat.getMaxId(), pageSize);
    for (long pageNum = 0; pageNum < stat.getCount(); pageNum+= pageSize) {
      List<Map<String, Object>> fields = jdbcTemplate.queryForList(coldStart.select(repo, pageNum, pageSize));
      SyncData[] syncData = coldStart.fromSqlRes(repo, fields, nowDataId);
      producerSink.output(syncData);
    }
    log.info("[Cold done] [{}.{}] count({})", repo, entity, stat.getCount());
    log.info("[Flush hold]");
    producerSink.markColdStartDoneAndFlush();
  }

  @Getter
  @Setter
  @ToString
  public static class ColdStartStatistic {
    private Object minId;
    private Object maxId;
    private long count;
  }


}
