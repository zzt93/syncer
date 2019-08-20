package com.github.zzt93.syncer.producer.input;

import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.consumer.ConsumerSource;

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

  public Consumer(ConsumerSource consumerSource) {
    this.repos = consumerSource.copyRepos();
    id = consumerSource.clientId();
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
}
