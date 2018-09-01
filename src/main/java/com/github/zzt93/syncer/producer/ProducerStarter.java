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
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.connect.MysqlMasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
public class ProducerStarter implements Starter<ProducerInput, Set<ProducerMaster>> {

  private static ProducerStarter starter;
  private final Logger logger = LoggerFactory.getLogger(ProducerStarter.class);
  private final Set<ProducerMaster> masterSources;
  private final ExecutorService service;
  private final ConsumerRegistry consumerRegistry;

  private ProducerStarter(ProducerInput input,
      SyncerInput syncerConfigInput, ConsumerRegistry consumerRegistry) {
    masterSources = fromPipelineConfig(input);
    int size = masterSources.size();
    if (size > Runtime.getRuntime().availableProcessors()) {
      logger.warn("Too many master source: {} > cores", size);
    }
    service = Executors.newFixedThreadPool(size, new NamedThreadFactory("syncer-producer"));
    this.consumerRegistry = consumerRegistry;
  }

  public static ProducerStarter getInstance(ProducerInput input, SyncerInput syncerConfigInput,
      ConsumerRegistry consumerRegistry) {
    if (starter == null) {
      starter = new ProducerStarter(input, syncerConfigInput, consumerRegistry);
    }
    return starter;
  }

  @Override
  public Starter start() throws IOException {
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
            masterConnector = new MysqlMasterConnector(new MysqlConnection(connection),
                masterSource.getFile(), consumerRegistry);
            break;
          case Mongo:
            masterConnector = new MongoMasterConnector(new MongoConnection(connection),
                consumerRegistry);
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
    return this;
  }


  @Override
  public Set<ProducerMaster> fromPipelineConfig(ProducerInput input) {
    return input.masterSet();
  }

  @Override
  public void close() throws Exception {
    service.shutdownNow();
    if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
      logger.error("Fail to shutdown producer");
    }
  }

  @Override
  public void registerToHealthCenter() {
    for (ProducerMaster source : masterSources) {
      SyncerHealth.producer(source.getConnection().connectionIdentifier(), Health.green());
    }
  }
}
