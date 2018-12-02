package com.github.zzt93.syncer.config.consumer.common;

import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.config.consumer.input.SyncMeta;
import com.github.zzt93.syncer.consumer.input.SchedulerBuilder.SchedulerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author zzt
 */
public class MasterSource {

  private final Logger logger = LoggerFactory.getLogger(MasterSource.class);
  private final Set<Repo> repoSet = new HashSet<>();
  private MasterSourceType type = MasterSourceType.MySQL;
  private SchedulerType scheduler = SchedulerType.hash;
  private SyncMeta syncMeta;
  private Connection connection;
  private List<Repo> repos = new ArrayList<>();

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public List<Repo> getRepos() {
    return repos;
  }

  public void setRepos(List<Repo> repos) {
    this.repos = repos;
    repoSet.addAll(repos);
    if (repoSet.size() < repos.size()) {
      logger.error("Duplicate repos: {}", repos);
      throw new InvalidConfigException("Duplicate repos");
    }
  }

  public Set<Repo> getRepoSet() {
    return repoSet;
  }

  public MasterSourceType getType() {
    return type;
  }

  public void setType(MasterSourceType type) {
    this.type = type;
  }

  public void setSyncMeta(SyncMeta syncMeta) {
    this.syncMeta = syncMeta;
  }

  public boolean hasSyncMeta() {
    return syncMeta != null && type == MasterSourceType.MySQL;
  }

  public SyncMeta getSyncMeta() {
    return syncMeta;
  }

  public void setScheduler(SchedulerType scheduler) {
    this.scheduler = scheduler;
  }

  public SchedulerType getScheduler() {
    return scheduler;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MasterSource that = (MasterSource) o;

    return connection.equals(that.connection);
  }

  @Override
  public int hashCode() {
    return connection.hashCode();
  }

  @Override
  public String toString() {
    return "MasterSource{" +
        "connection=" + connection +
        ", repos=" + repos +
        ", type=" + type +
        '}';
  }

}
