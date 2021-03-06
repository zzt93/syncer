package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.ColdStartDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static com.github.zzt93.syncer.common.util.RegexUtil.regexToLike;

/**
 * @author zzt
 */
@Slf4j
public class MysqlColdStartConnector implements MasterConnector {
  private final JdbcTemplate jdbcTemplate;
  private final List<ColdStart> colds;
  private final JdbcRowMapper rowMapper;

  public MysqlColdStartConnector(MysqlConnection connection, List<ColdStart> colds) {
    jdbcTemplate = new JdbcTemplate(connection.dataSource());
    rowMapper = new JdbcRowMapper();
    this.colds = colds;
  }

  @Getter
  @Setter
  private static class MySQLMasterStatus {
    private String file;
    private long position;
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
        List<String> dbs = jdbcTemplate.queryForList(String.format("show databases like '%s'", regexToLike(coldStart.getRepo())), String.class);
        for (String db : dbs) {
          coldStart(coldStart.getProducerSink(), coldStart, db);
        }
      }
    }
    log.info("[Cold done] take {}", System.currentTimeMillis() - start);
  }

  private void coldStart(ProducerSink producerSink, ColdStart coldStart, String repo) {
    String entity = coldStart.getEntity(), pkName = coldStart.getPkName();
    int pageSize = coldStart.getPageSize();
    producerSink.markColdStart(repo, entity);

    String sql = coldStart.statSql(repo);
    ColdStartStatistic stat = jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<>(ColdStartStatistic.class));
    if (stat == null) {
      throw new InvalidConfigException(coldStart.toString());
    } else if (stat.isEmpty()) {
      log.info("No data for {}. {} done.", repo, coldStart);
      return;
    }

    log.info("[Cold start] [{}.{}] count({}) {} [{}, {}] pageSize={}", repo, entity, stat.getCount(), pkName, stat.getMinId(), stat.getMaxId(), pageSize);
    for (long offset = 0; offset < stat.getCount(); offset += pageSize) {
      List<Map<String, Object>> fields = jdbcTemplate.query(coldStart.select(repo, offset, pageSize), rowMapper);
      SyncData[] syncData = coldStart.fromSqlRes(repo, fields, ColdStartDataId.BINLOG_COLD);
      producerSink.coldOutput(syncData);
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

    public boolean isEmpty() {
      return count == 0;
    }
  }


}
