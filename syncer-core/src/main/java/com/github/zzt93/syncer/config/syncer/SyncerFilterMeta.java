package com.github.zzt93.syncer.config.syncer;

import com.github.zzt93.syncer.common.util.FileUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zzt
 */
@ConfigurationProperties(prefix = "syncer.filter.filterMeta")
public class SyncerFilterMeta {

  private String src = "./src/";

  public String getSrc() {
    return src;
  }

  public void setSrc(String src) {
    this.src = src;
    FileUtil.createDirIfNotExist(src);
  }

}
