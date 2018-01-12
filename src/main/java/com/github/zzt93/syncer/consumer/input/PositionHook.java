package com.github.zzt93.syncer.consumer.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class PositionHook implements Runnable {

  private final Ack ack;
  private final Logger logger = LoggerFactory.getLogger(PositionHook.class);

  PositionHook(Ack ack) {
    this.ack = ack;
  }

  @Override
  public void run() {
    for (Entry<String, Path> entry : ack.connectorMetaPath().entrySet()) {
      List<String> lines = ack.connectorMeta(entry.getKey());
      if (lines.get(0) == null) {
        logger.info("master connector not connected to any binlog file, no info recorded");
        return;
      }
      Path lastRunMetaPath = entry.getValue();
      try {
        Files.write(lastRunMetaPath, lines, StandardCharsets.UTF_8);
      } catch (IOException e) {
        logger.error("Fail to write last run meta info of connector:{}", lastRunMetaPath, e);
      }
    }

  }
}
