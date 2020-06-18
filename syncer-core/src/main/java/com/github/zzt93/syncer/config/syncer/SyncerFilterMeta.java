package com.github.zzt93.syncer.config.syncer;

import com.github.zzt93.syncer.common.util.FileUtil;
import lombok.Getter;

/**
 * @author zzt
 */
@Getter
public class SyncerFilterMeta {

  private String src = "./src/";
  private String failureLogDir = "./failure/";

  public void setSrc(String src) {
    this.src = src;
    FileUtil.createDirIfNotExist(src);
  }

  public void setFailureLogDir(String failureLogDir) {
    this.failureLogDir = failureLogDir;
    FileUtil.createDirIfNotExist(failureLogDir);
  }
}
