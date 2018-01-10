package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.syncer.SyncerMysql;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class Registrant implements Callable<Boolean> {

  private static final Logger logger = LoggerFactory.getLogger(Registrant.class);
  private final Path connectorMetaPath;

  public Registrant(ConsumerRegistry consumerRegistry,
      SyncerMysql syncerMysql, String identifier,
      MysqlMaster mysqlMaster, String clientId) throws IOException {
    connectorMetaPath = Paths
        .get(syncerMysql.getLastRunMetadataDir(), identifier, "");
    if (!Files.exists(connectorMetaPath)) {
      logger.info("Last run meta file not exists, fresh run");
    } else {
      List<String> lines = Files.readAllLines(connectorMetaPath, StandardCharsets.UTF_8);
      if (lines.size() == 2) {
        BinlogInfo binlogInfo = new BinlogInfo(lines.get(0), Long.parseLong(lines.get(1)));
        LocalInputSource inputSource = new LocalInputSource(consumerRegistry,
            mysqlMaster.getSchemas(),
            mysqlMaster.getConnection(), binlogInfo, clientId);
      }
    }
//    consumerRegistry.register()
  }

  @ThreadSafe(des = "final field is thread safe: it is fixed before hook thread start")
  Path connectorMetaPath() {
    return connectorMetaPath;
  }

  @Override
  public Boolean call() throws Exception {
    return null;
  }
}
