package com.github.zzt93.syncer.producer;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.common.MongoConnection;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.producer.ProducerInput;
import com.github.zzt93.syncer.config.producer.ProducerMaster;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.connect.MysqlMasterConnector;
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
public class ProducerStarter implements Starter<ProducerInput,Set<ProducerMaster>> {

  private static ProducerStarter starter;
  private final Logger logger = LoggerFactory.getLogger(ProducerStarter.class);
  private final Set<ProducerMaster> masterSources;
  private final ExecutorService service;
  private final ConsumerRegistry consumerRegistry;
  private final int maxRetry;

  private ProducerStarter(ProducerInput input,
      SyncerInput syncerConfigInput, ConsumerRegistry consumerRegistry) {
    masterSources = fromPipelineConfig(input);
    service = Executors
        .newFixedThreadPool(syncerConfigInput.getWorker(),
            new NamedThreadFactory("syncer-producer"));
    this.consumerRegistry = consumerRegistry;
    maxRetry = syncerConfigInput.getMaxRetry();
  }

  public static ProducerStarter getInstance(ProducerInput input, SyncerInput syncerConfigInput,
      ConsumerRegistry consumerRegistry) {
    if (starter == null) {
      starter = new ProducerStarter(input, syncerConfigInput, consumerRegistry);
    }
    return starter;
  }

  @Override
  public void start() throws IOException {
    logger.info("Start connecting to [{}]", masterSources);
    if (masterSources.size() > 1) {
      logger.warn("Connect to multiple masters, not suggested usage");
    }

    Set<Connection> wanted = consumerRegistry.wantedSource();
    for (ProducerMaster masterSource : masterSources) {
      Connection connection = masterSource.getConnection();
      wanted.remove(connection);
      if (consumerRegistry.outputSink(connection).isEmpty()) {
        logger.warn("Skip {} because no consumer registered", masterSource);
        continue;
      }
      try {
        MasterConnector masterConnector = null;
        switch (masterSource.getType()) {
          case MySQL:
            masterConnector = new MysqlMasterConnector(new MysqlConnection(connection), consumerRegistry, maxRetry);
            break;
          case Mongo:
            masterConnector = new MongoMasterConnector(new MongoConnection(connection), consumerRegistry, maxRetry);
            break;
        }
        service.submit(masterConnector);
      } catch (IOException | SchemaUnavailableException e) {
        logger.error("Fail to connect to master source: {}", masterSource, e);
      }
    }
    if (!wanted.isEmpty()) {
      logger.warn("Some consumer wanted source is not configured in `producer`: {}", wanted);
    }
  }


  @Override
  public Set<ProducerMaster> fromPipelineConfig(ProducerInput input) {
    return input.masterSet();
  }
}
