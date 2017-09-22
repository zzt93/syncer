package com.github.zzt93.syncer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.input.connect.MasterConnector;
import com.github.zzt93.syncer.input.connect.NamedThreadFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class InputStarter implements Starter<PipelineInput, Set<MysqlMaster>> {

  private static InputStarter instance;
  private final ExecutorService service;
  private final BlockingQueue<SyncData> queue;
  private final Logger logger = LoggerFactory.getLogger(InputStarter.class);
  private final Set<MysqlMaster> mysqlMasters;

  private InputStarter(PipelineInput pipelineInputConfig, SyncerInput input, BlockingQueue<SyncData> queue) {
    this.queue = queue;
    mysqlMasters = fromPipelineConfig(pipelineInputConfig);
    service = Executors.newFixedThreadPool(input.getWorker(), new NamedThreadFactory("syncer-input"));
  }

  public static InputStarter getInstance(PipelineInput pipelineInputConfig, SyncerInput input,
      BlockingQueue<SyncData> queue) {
    if (instance == null) {
      instance = new InputStarter(pipelineInputConfig, input, queue);
    }
    return instance;
  }

  public void start() throws IOException {
    logger.info("Start connecting to input source {}", mysqlMasters);
    for (MysqlMaster mysqlMaster : mysqlMasters) {
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

  @Override
  public Set<MysqlMaster> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMysqlMasterSet();
  }
}
