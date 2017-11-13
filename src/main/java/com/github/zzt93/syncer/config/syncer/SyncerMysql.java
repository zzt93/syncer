package com.github.zzt93.syncer.config.syncer;

/**
 * @author zzt
 */
public class SyncerMysql {

  private String lastRunMetaPath = "./";

  public String getLastRunMetaPath() {
    return lastRunMetaPath;
  }

  public SyncerMysql setLastRunMetaPath(String lastRunMetaPath) {
    this.lastRunMetaPath = lastRunMetaPath;
    return this;
  }
}
