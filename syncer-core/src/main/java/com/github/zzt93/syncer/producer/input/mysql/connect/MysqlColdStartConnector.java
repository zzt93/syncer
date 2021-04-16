package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.BinlogDataId;
import com.github.zzt93.syncer.common.data.DataId;
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
  private final MysqlConnection connection;
  private DataId nowDataId;

  public MysqlColdStartConnector(MysqlConnection connection, List<ColdStart> colds) {
    this.connection = connection;
    jdbcTemplate = new JdbcTemplate(this.connection.dataSource());
    this.colds = colds;
    nowDataId = init(jdbcTemplate);
  }

  @Getter
  @Setter
  private static class MySQLMasterStatus {
    private String file;
    private long position;
  }

  private DataId init(JdbcTemplate jdbcTemplate) {
    MySQLMasterStatus status = jdbcTemplate.queryForObject("show master status", new BeanPropertyRowMapper<>(MySQLMasterStatus.class));
    if (status == null) {
      throw new InvalidConfigException("Fail to fetch binlog info by `show master status`");
    }
    return new BinlogDataId(status.getFile(), status.getPosition(), status.getPosition());
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

    public boolean isEmpty() {
      return count == 0;
    }
  }


}
