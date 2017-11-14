package com.github.zzt93.syncer.config.syncer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncerMysql {

  private Logger logger = LoggerFactory.getLogger(SyncerMysql.class);
  private String lastRunMetadataDir = ".";

  public String getLastRunMetadataDir() {
    return lastRunMetadataDir;
  }

  public void setLastRunMetadataDir(String lastRunMetadataDir) throws IOException {
    this.lastRunMetadataDir = lastRunMetadataDir;
    Path metaDir = Paths.get(lastRunMetadataDir);
    if (!Files.exists(metaDir)) {
      logger.info("last_run_metadata_dir({}) not exists, create a new one", lastRunMetadataDir);
      Files.createDirectories(metaDir);
    }
  }
}
