package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.input.Entity;
import com.github.zzt93.syncer.config.consumer.input.Fields;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.SchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.TableMeta;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString
public class ColdStart {
  private String repo;
  private String entity;
  private String pkName;

  private Fields fields;

  private int pageSize;
  private String where;

  private ProducerSink producerSink;

  private ColdStart(SchemaMeta schemaMeta, TableMeta tableMeta, ProducerSink value) {
    repo = schemaMeta.getSchema();
    Entity coldStart = tableMeta.coldStart();
    entity = coldStart.getName();
    pkName = tableMeta.getPrimaryKeyName();
    fields = coldStart.getField();
    pageSize = coldStart.getCold().getPageSize();
    where = coldStart.getCold().getWhere();
    producerSink = value;
  }

  public static Collection<ColdStart> from(ConsumerSchemaMeta key, ProducerSink value) {
    List<ColdStart> res = new ArrayList<>();
    for (SchemaMeta schemaMeta : key.coldStart()) {
      for (TableMeta tableMeta : schemaMeta.codeStart()) {
        res.add(new ColdStart(schemaMeta, tableMeta, value));
      }
    }
    return res;
  }

  public String select(String repo, long page, int pageSize) {
    String field = fields.toSql();
    if (where == null) {
      return String.format("select %s from `%s`.`%s` limit %s,%s", field, repo, entity, page * pageSize, pageSize);
    }
    return String.format("select %s from `%s`.`%s` where %s limit %s,%s", field, repo, entity, where, page * pageSize, pageSize);
  }

  public SyncData[] fromSqlRes(String repo, List<Map<String, Object>> fields, DataId nowDataId) {
    SyncData[] res = new SyncData[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      Map<String, Object> f = fields.get(i);
      res[i] = new SyncData(nowDataId, SimpleEventType.WRITE, repo, getEntity(), getPkName(), f.get(getPkName()), new NamedFullRow(f));
    }
    return res;
  }

  public boolean isDrds() {
    return repo.contains(".*");
  }
}
