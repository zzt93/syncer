package com.github.zzt93.syncer.config.syncer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.output.outputMeta")
public class SyncerOutputMeta {

  private String failureLogDir = "./failure/";


  public String getFailureLogDir() {
    return failureLogDir;
  }

  public void setFailureLogDir(String failureLogDir) {
    this.failureLogDir = failureLogDir;
  }
}
