package com.github.zzt93.syncer.input;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.pipeline.input.Input;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.syncer.InputModule;
import com.github.zzt93.syncer.input.connect.MasterConnector;
import com.github.zzt93.syncer.input.connect.NamedThreadFactory;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class InputStarter {

  private static InputStarter instance;
  private final ExecutorService service;
  private final BlockingQueue<SyncData> queue;
  private final Input inputConfig;
  private final Logger logger = LoggerFactory.getLogger(InputStarter.class);

  private InputStarter(Input inputConfig, InputModule input, BlockingQueue<SyncData> queue) {
    this.inputConfig = inputConfig;
    this.queue = queue;
    service = Executors.newFixedThreadPool(input.getWorker(), new NamedThreadFactory());
  }

  public static InputStarter getInstance(Input inputConfig, InputModule input,
      BlockingQueue<SyncData> queue) {
    if (instance == null) {
      instance = new InputStarter(inputConfig, input, queue);
    }
    return instance;
  }

  public void start() throws IOException {
    logger.info("Start connecting to input source {}", inputConfig);
    for (MysqlMaster mysqlMaster : inputConfig.getMysqlMasterSet()) {
      try {
        MasterConnector masterConnector = new MasterConnector(mysqlMaster.getConnection(),
            mysqlMaster.getSchema(), queue);
        service.submit(masterConnector);
      } catch (IOException | SchemaUnavailableException e) {
        logger.error("Fail to connect to mysql endpoint: {}", mysqlMaster);
        logger.error("", e);
      }
    }
  }
}
