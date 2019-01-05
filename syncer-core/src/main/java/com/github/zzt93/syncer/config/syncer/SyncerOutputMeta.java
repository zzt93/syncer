package com.github.zzt93.syncer.config.syncer;


/**
 * @author zzt
 */
public class SyncerOutputMeta {

  private String failureLogDir = "./failure/";


  public String getFailureLogDir() {
    return failureLogDir;
  }

  public void setFailureLogDir(String failureLogDir) {
    this.failureLogDir = failureLogDir;
  }
}
