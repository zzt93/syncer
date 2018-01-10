package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
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

  public InputStarter(PipelineInput pipelineInput, SyncerInput input,
      ConsumerRegistry consumerRegistry)
      throws IOException {
    mysqlMasters = fromPipelineConfig(pipelineInput);
    service = Executors
        .newFixedThreadPool(input.getWorker(), new NamedThreadFactory("syncer-input"));

    int i = 0;
    for (MysqlMaster mysqlMaster : pipelineInput.getMysqlMasterSet()) {
      String identifier = mysqlMaster.getConnection().connectionIdentifier();
      registrant = new Registrant(consumerRegistry, input.getMysqlMasters(), identifier, mysqlMaster, "" + i++);
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
