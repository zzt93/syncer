package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import java.io.IOException;
import java.util.Set;
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
  private Registrant registrant;
  private Ack ack;

  public InputStarter(PipelineInput pipelineInputConfig, SyncerInput input)
      throws IOException {
    mysqlMasters = fromPipelineConfig(pipelineInputConfig);
    service = Executors
        .newFixedThreadPool(input.getWorker(), new NamedThreadFactory("syncer-input"));

    for (MysqlMaster mysqlMaster : pipelineInputConfig.getMysqlMasterSet()) {
      String identifier = mysqlMaster.getConnection().connectionIdentifier();
      registrant = new Registrant(input.getMysqlMasters(), identifier);
      ack = new Ack();
    }

  }

  public void start() throws IOException {
    logger.info("Start connecting to input source {}", mysqlMasters);
    service.submit(registrant);
    service.submit(ack);
    // final field in master connector is thread safe: it is fixed before thread start
    Runtime.getRuntime().addShutdownHook(new Thread(new PositionHook(registrant, ack)));
  }

  @Override
  public Set<MysqlMaster> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMysqlMasterSet();
  }
}
