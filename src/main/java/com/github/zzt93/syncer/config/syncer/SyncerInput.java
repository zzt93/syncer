package com.github.zzt93.syncer.config.syncer;

/**
 * @author zzt
 */
public class SyncerInput {

  private int worker;
  private SyncerMysql mysqlMasters;

  public SyncerMysql getMysqlMasters() {
    return mysqlMasters;
  }

  public void setMysqlMasters(SyncerMysql mysqlMasters) {
    this.mysqlMasters = mysqlMasters;
  }

  public int getWorker() {
    return worker;
  }

  public void setWorker(int worker) {
    this.worker = worker;
  }
}
