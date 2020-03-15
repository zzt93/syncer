package com.github.zzt93.syncer.producer.input;

import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.input.Entity;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.github.zzt93.syncer.consumer.input.MysqlLocalConsumerSource;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;

import java.util.Objects;
import java.util.Set;

/**
 * All repos a consumer requested for a remote DB.
 *
 * @see MasterSource#getRepoSet()
 * @author zzt
 */
public class Consumer {

  private final Set<Repo> repos;
  private final String id;
  private final ConsumerSource consumerSource;

  public Consumer(ConsumerSource consumerSource) {
    this.repos = consumerSource.copyRepos();
    id = consumerSource.clientId();
    this.consumerSource = consumerSource;
  }

  private Consumer(Set<Repo> repos, String id) {
    this.repos = repos;
    this.id = id;
    consumerSource = null;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Consumer consumer = (Consumer) o;
    return Objects.equals(id, consumer.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public Set<Repo> getRepos() {
    return repos;
  }

  @Override
  public String toString() {
    return "Consumer{" +
        "id='" + id + '\'' +
        ", repos(" + repos.size() + ")" + "=" + repos +
        '}';
  }

  public Repo matchedSchema(String tableSchema, String tableName) {
    for (Repo aim : getRepos()) {
      if (aim.contain(tableSchema, tableName)) {
        // a consumer should only match one table at one time
        return aim;
      }
    }
    return null;
  }

  public static Consumer singleTable(String schema, String table) {
    return new Consumer(Sets.newHashSet(new Repo(schema, Lists.newArrayList(new Entity(table)))), "single");
  }

  public boolean isMysqlLatest() {
    return consumerSource instanceof MysqlLocalConsumerSource
        && ((MysqlLocalConsumerSource) consumerSource).getSyncInitMeta() == BinlogInfo.latest;
  }

  public void replaceLatest(BinlogInfo nowLatest) {
    ((MysqlLocalConsumerSource) consumerSource).replaceLatest(nowLatest);
  }
}
