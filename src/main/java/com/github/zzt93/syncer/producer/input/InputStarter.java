package com.github.zzt93.syncer.producer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
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

  private final ExecutorService service;
  private final Logger logger = LoggerFactory.getLogger(InputStarter.class);
  private final Set<MysqlMaster> mysqlMasters;
  private final SyncerInput input;

  public InputStarter(PipelineInput pipelineInputConfig, SyncerInput input,
      BlockingQueue<SyncData> queue) {
    mysqlMasters = fromPipelineConfig(pipelineInputConfig);
    service = Executors
        .newFixedThreadPool(input.getWorker(), new NamedThreadFactory("syncer-input"));
    this.input = input;
  }

  public void start() throws IOException {
    logger.info("Start connecting to input source {}", mysqlMasters);
  }

  @Override
  public Set<MysqlMaster> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMysqlMasterSet();
  }
}
