package com.github.zzt93.syncer.producer;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.producer.input.InputStarter;
import com.github.zzt93.syncer.producer.input.connect.MasterConnector;
import com.github.zzt93.syncer.producer.input.connect.PositionHook;
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
public class ProducerStarter implements Starter<PipelineInput, Set<MysqlMaster>> {

  private static ProducerStarter starter;
  private final Logger logger = LoggerFactory.getLogger(InputStarter.class);
  private final Set<MysqlMaster> mysqlMasters;
  private final ExecutorService service;
  private final ConsumerRegistry consumerRegistry;

  private ProducerStarter(PipelineInput input,
      SyncerInput syncerConfigInput, ConsumerRegistry consumerRegistry) {
    mysqlMasters = fromPipelineConfig(input);
    service = Executors
        .newFixedThreadPool(syncerConfigInput.getWorker(),
            new NamedThreadFactory("syncer-producer"));
    this.consumerRegistry = consumerRegistry;
  }

  public static ProducerStarter getInstance(PipelineInput input, SyncerInput syncerConfigInput,
      ConsumerRegistry consumerRegistry) {
    if (starter == null) {
      starter = new ProducerStarter(input, syncerConfigInput, consumerRegistry);
    }
    return starter;
  }

  @Override
  public void start() throws IOException {
    logger.info("Start connecting to input source {}", mysqlMasters);
    if (mysqlMasters.size() > 1) {
      logger.warn("Connect to multiple masters, may affect performance");
    }
    for (MysqlMaster mysqlMaster : mysqlMasters) {
      if (mysqlMaster.getSchemas().isEmpty()) {
        logger.warn("No schema specified, ignore this connection {}", mysqlMaster.getConnection());
        continue;
      }
      try {
        MasterConnector masterConnector = new MasterConnector(mysqlMaster.getConnection(),
            mysqlMaster.getSchemas(), consumerRegistry);
        // final field in master connector is thread safe: it is fixed before thread start
        Runtime.getRuntime().addShutdownHook(
            new Thread(new PositionHook(masterConnector)));
        service.submit(masterConnector);
      } catch (IOException | SchemaUnavailableException e) {
        logger.error("Fail to connect to mysql endpoint: {}", mysqlMaster, e);
      }
    }
  }


  @Override
  public Set<MysqlMaster> fromPipelineConfig(PipelineInput input) {
    return input.getMysqlMasterSet();
  }
}
