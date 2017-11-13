package com.github.zzt93.syncer.input.connect;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class PositionHook implements Runnable {

  private final MasterConnector masterConnector;
  private final Path lastRunMetaPath;
  private Logger logger = LoggerFactory.getLogger(PositionHook.class);

  public PositionHook(MasterConnector masterConnector) {
    this.masterConnector = masterConnector;
    lastRunMetaPath = masterConnector.connectorMetaPath();
  }

  @Override
  public void run() {
    try {
      Files.write(lastRunMetaPath,
          masterConnector.connectorMeta(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Fail to write last run meta info of connector:{}", lastRunMetaPath, e);
    }
  }
}
