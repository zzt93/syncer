package com.github.zzt93.syncer.config.syncer;

import com.github.zzt93.syncer.common.util.FileUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.input.inputMeta")
public class SyncerInputMeta {

  private String lastRunMetadataDir = "./last_position/";

  public String getLastRunMetadataDir() {
    return lastRunMetadataDir;
  }

  public void setLastRunMetadataDir(String lastRunMetadataDir) {
    this.lastRunMetadataDir = lastRunMetadataDir;
    FileUtil.createDirIfNotExist(lastRunMetadataDir);
  }

}
