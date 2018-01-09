package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.input.connect.MasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author zzt
 */
@Component
public class LocalInputSource implements InputSource {

  private Logger logger = LoggerFactory.getLogger(MasterConnector.class);

  @Autowired
  private ConsumerRegistry consumerRegistry;

  private List<Schema> schemas;
  private BinlogInfo binlogInfo;

  // TODO 18/1/8 constructor
  public LocalInputSource(List<Schema> schemas) throws IOException {
    this.schemas = schemas;
    Path connectorMetaPath = Paths
        .get(mysqlMastersMeta.getLastRunMetadataDir(), connectorIdentifier);
    if (!Files.exists(connectorMetaPath)) {
      logger.info("Last run meta file not exists, fresh run");
    } else {
      List<String> lines = Files.readAllLines(connectorMetaPath, StandardCharsets.UTF_8);
      if (lines.size() == 2) {
        binlogInfo = new BinlogInfo(lines.get(0), (Long.parseLong(lines.get(1));
      }
    }
  }

  @Override
  public boolean register() {
    return consumerRegistry.register(this);
  }

  @Override
  public BinlogInfo getBinlogInfo() {
    return binlogInfo;
  }

  @Override
  public List<Schema> getSchemas() {
    return schemas;
  }

  @Override
  public String clientId() {
    return null;
  }

  @Override
  public boolean input(SyncData data) {
    return false;
  }

  @Override
  public boolean input(SyncData[] data) {
    return false;
  }


  @Override
  public int compareTo(InputSource o) {
    return 0;
  }
}
