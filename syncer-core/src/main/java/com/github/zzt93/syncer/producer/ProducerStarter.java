package com.github.zzt93.syncer.producer;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.common.*;
import com.github.zzt93.syncer.config.producer.ProducerInput;
import com.github.zzt93.syncer.config.producer.ProducerMaster;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnectorFactory;
import com.github.zzt93.syncer.producer.input.mysql.connect.MysqlMasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
public class ProducerStarter implements Starter {

  private static ProducerStarter starter;
  private final Logger logger = LoggerFactory.getLogger(ProducerStarter.class);
  private final Set<ProducerMaster> masterSources;
  private final ExecutorService service;
  private final ConsumerRegistry consumerRegistry;
  private final LinkedList<MasterConnector> connectors = new LinkedList<>();

  private ProducerStarter(ProducerInput input,
      SyncerInput syncerConfigInput, ConsumerRegistry consumerRegistry) {
    masterSources = fromPipelineConfig(input);
    int size = masterSources.stream().mapToInt(p->p.getRealConnection().getReals().size()).sum();
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
    logger.info("Start handling [{}]", masterSources);
    if (masterSources.size() > 1) {
      logger.warn("Connect to multiple masters, not suggested usage");
    }

    Set<Connection> wanted = consumerRegistry.wantedSource();
    for (ProducerMaster masterSource : masterSources) {
      Connection mayClusterConnection = masterSource.getRealConnection();
      for (Connection real : mayClusterConnection.getReals()) {
        wanted.remove(real);
        addConnector(masterSource, real);
      }
    }
    if (!wanted.isEmpty()) {
      logger.warn("Some consumer wanted source is not configured in `producer`: {}", wanted);
    }
    return this;
  }

  private void addConnector(ProducerMaster masterSource, Connection connection) {
    if (consumerRegistry.outputSink(connection).isEmpty()) {
      logger.warn("Skip {} because no consumer registered", masterSource);
      return;
    }
    try {
      MasterConnector masterConnector = null;
      switch (masterSource.getType()) {
        case MySQL:
          masterConnector = new MysqlMasterConnector(new MysqlConnection(connection),
              masterSource.getFile(), consumerRegistry, masterSource.isOnlyUpdated());
          break;
        case Mongo:
          masterConnector = new MongoMasterConnectorFactory(new MongoConnection(connection),
              consumerRegistry).getMongoConnectorByServerVersion();
          break;
      }
      connectors.add(masterConnector);
      service.submit(masterConnector);
    } catch (InvalidConfigException e) {
      logger.error("Invalid config for {}", masterSource);
      ShutDownCenter.initShutDown(e);
    } catch (IOException | SchemaUnavailableException e) {
      logger.error("Fail to connect to master source: {}", masterSource);
      ShutDownCenter.initShutDown(e);
    }
  }


  private Set<ProducerMaster> fromPipelineConfig(ProducerInput input) {
    return input.masterSet();
  }

  @Override
  public void close() throws Exception {
    for (MasterConnector connector : connectors) {
      connector.close();
    }

    service.shutdownNow();
    while (!service.awaitTermination(ShutDownCenter.SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
      logger.warn("[Shutting down] producer");
      service.shutdownNow();
    }
  }

  @Override
  public void registerToHealthCenter() {
    for (ProducerMaster source : masterSources) {
      Connection connection = source.getRealConnection();
      for (Connection real : connection.getReals()) {
        if (consumerRegistry.outputSink(real).isEmpty()) {
          SyncerHealth.producer(real.connectionIdentifier(), Health.inactive("No consumer registered"));
        } else {
          SyncerHealth.producer(real.connectionIdentifier(), Health.green());
        }
      }
    }
  }
}
