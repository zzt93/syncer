package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.ColdStartConfig;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author zzt
 */
@Slf4j
public class MysqlColdStartConnector implements MasterConnector {
  private MysqlMasterConnector mysqlMasterConnector;
  private JdbcTemplate jdbcTemplate;
  private ArrayBlockingQueue<SyncData> holds = new ArrayBlockingQueue<>(1);
  private List<ColdStart> colds;
  private ConsumerChannel consumerChannel;

  @Override
  public void close() {

  }

  @Override
  public void run() {

  }

  @Override
  public void loop() {
    log.debug("Cold start start at {}", System.currentTimeMillis());
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
    log.debug("Cold start done at {}", System.currentTimeMillis());
  }

  private void coldStart(ColdStart coldStart, String repo) {
    String entity = coldStart.getEntity(), pkName = coldStart.getPkName();
    int pageSize = coldStart.getPageSize();

    String sql = String.format("select min(%s) as minId, max(%s) as maxId, count(1) as count from `%s`.`%s`", pkName, pkName, repo, entity);
    ColdStartStatistic stat = jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<>(ColdStartStatistic.class));
    if (stat == null) {
      throw new InvalidConfigException(coldStart.toString());
    }

    log.info("[Cold start] {}.{} count({}) id [{}, {}] pageSize={}", repo, entity, stat.getCount(), stat.getMinId(), stat.getMaxId(), pageSize);
    for (long pageNum = 0; pageNum < stat.getCount(); pageNum+= pageSize) {
      List<Map<String, Object>> fields = jdbcTemplate.queryForList(coldStart.select(repo, pageNum, pageSize));
      SyncData[] syncData = coldStart.fromSqlRes(fields, repo);
      consumerChannel.output(syncData);
    }
    log.info("[Cold done] {}.{} count({})", repo, entity, stat.getCount());
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
