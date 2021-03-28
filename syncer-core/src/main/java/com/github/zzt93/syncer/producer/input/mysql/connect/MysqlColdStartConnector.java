package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.input.Fields;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.ConsumerChannel;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
@Slf4j
public class MysqlColdStartConnector implements MasterConnector {
  private JdbcTemplate jdbcTemplate;
  private List<ColdStart> colds;
  private final List<ConsumerChannel> consumerChannels;

  public MysqlColdStartConnector(List<ConsumerChannel> consumerChannels) {
    this.consumerChannels = consumerChannels;
  }

  @Override
  public void close() {

  }

  @Override
  public void run() {

  }

  @Override
  public void loop() {
    long start = System.currentTimeMillis();
    log.info("[Cold start] at {}", start);
    for (ColdStart coldStart : colds) {
      if (!coldStart.isCluster()) {
        String repo = coldStart.getRepoOrRegex();
        coldStart(coldStart, repo);
      } else {
        for (String repo : coldStart.getRepos()) {
          coldStart(coldStart, repo);
        }
      }
    }
    log.info("[Cold done] take {}", System.currentTimeMillis() - start);
  }

  private void coldStart(ColdStart coldStart, String repo) {
    String entity = coldStart.getEntity(), pkName = coldStart.getPkName();
    for (ConsumerChannel consumerChannel : consumerChannels) {
      consumerChannel.markColdStart(repo, entity);
    }
    int pageSize = coldStart.getPageSize();

    String sql = String.format("select min(%s) as minId, max(%s) as maxId, count(1) as count from `%s`.`%s`", pkName, pkName, repo, entity);
    ColdStartStatistic stat = jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<>(ColdStartStatistic.class));
    if (stat == null) {
      throw new InvalidConfigException(coldStart.toString());
    }

    log.info("[Cold start] [{}.{}] count({}) {} [{}, {}] pageSize={}", repo, entity, stat.getCount(), pkName, stat.getMinId(), stat.getMaxId(), pageSize);
    for (long pageNum = 0; pageNum < stat.getCount(); pageNum+= pageSize) {
      List<Map<String, Object>> fields = jdbcTemplate.queryForList(coldStart.select(repo, pageNum, pageSize));
      SyncData[] syncData = coldStart.fromSqlRes(fields, repo);
      for (ConsumerChannel consumerChannel : consumerChannels) {
        consumerChannel.output(syncData);
      }
    }
    log.info("[Cold done] [{}.{}] count({})", repo, entity, stat.getCount());
    log.info("[Flush hold]");
    for (ConsumerChannel consumerChannel : consumerChannels) {
      consumerChannel.markColdStartDoneAndFlush();
    }
  }

  @Getter
  @Setter
  @ToString
  public static class ColdStartStatistic {
    private Object minId;
    private Object maxId;
    private long count;
  }

  @Getter
  @Setter
  @ToString
  public static class ColdStart {
    private List<String> repos;
    private String repoOrRegex;
    private String entity;
    private String pkName;

    private Fields fields;

    private DataId now;
    private int pageSize;
    private String where;

    public String select(String repo, long page, int pageSize) {
      String field = fields.toSql();
      if (where == null) {
        return String.format("select %s from `%s`.`%s` limit %s,%s", field, repo, entity, page*pageSize, pageSize);
      }
      return String.format("select %s from `%s`.`%s` where %s limit %s,%s", field, repo, entity, where, page*pageSize, pageSize);
    }

    public SyncData[] fromSqlRes(List<Map<String, Object>> fields, String repo) {
      SyncData[] res = new SyncData[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        Map<String, Object> f = fields.get(i);
        res[i] = new SyncData(getNow(), SimpleEventType.WRITE, repo, getEntity(), getPkName(), f.get(getPkName()), new NamedFullRow(f));
      }
      return res;
    }

    public boolean isCluster() {
      return repos != null;
    }
  }


}
