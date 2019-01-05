package com.github.zzt93.syncer.config.syncer;

import com.github.zzt93.syncer.common.util.FileUtil;

/**
 * @author zzt
 */
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
