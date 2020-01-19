package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MongoConnection;
import com.github.zzt93.syncer.config.consumer.input.Entity;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public abstract class MongoConnectorBase implements MasterConnector {
  private final String identifier;
  private final Logger logger = LoggerFactory.getLogger(MongoConnectorBase.class);
  protected MongoClient client;

  MongoConnectorBase(MongoConnection connection) {
    client = new MongoClient(new MongoClientURI(connection.toConnectionUrl(null)));
    identifier = connection.connectionIdentifier();
  }

  Pattern getNamespaces(MongoConnection connection, ConsumerRegistry registry) {
    Set<String> producerDbName = new HashSet<>();
    for (String dbName : client.listDatabaseNames()) {
      producerDbName.add(dbName);
    }

    Set<Consumer> consumers = registry.outputSink(connection).keySet();
    StringJoiner joiner = new StringJoiner("|");
    consumers.stream().map(Consumer::getRepos).flatMap(Set::stream).flatMap(s -> {
      if (!producerDbName.contains(s.getName())) {
        throw new InvalidConfigException("No such repo(" + s.getName() + ") in " + connection);
      }
      Set<String> producerCollectionName = new HashSet<>();
      for (String collectionName : client.getDatabase(s.getName()).listCollectionNames()) {
        producerCollectionName.add(collectionName);
      }

      List<Entity> entities = s.getEntities();
      ArrayList<String> res = new ArrayList<>(entities.size());
      for (Entity entity : entities) {
        if (!producerCollectionName.contains(entity.getName())) {
          throw new InvalidConfigException("No such collection(" + s.getName() + "." + entity.getName() + ") in " + connection);
        }
        res.add("(" + s.getName() + "\\." + entity.getName() + ")");
      }
      return res.stream();
    }).forEach(joiner::add);
    return Pattern.compile(joiner.toString());
  }


  @Override
  public void close() {
    try {
      closeCursor();
      client.close();
    } catch (Throwable e) {
      logger.error("[Shutting down] failed", e);
      return;
    }
    MasterConnector.super.close();
  }

  @Override
  public void loop() {
    Thread.currentThread().setName(identifier);
    logger.info("Start export from [{}]", identifier);

    long sleepInSecond = 1;
    while (!Thread.interrupted()) {
      try {
        configCursor();
        eventLoop();
      } catch (MongoTimeoutException | MongoSocketException e) {
        logger
            .error("Fail to connect to remote: {}, retry in {} second", identifier, sleepInSecond);
        try {
          sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
          TimeUnit.SECONDS.sleep(sleepInSecond);
        } catch (InterruptedException e1) {
          logger.error("Interrupt mongo {}", identifier, e1);
          Thread.currentThread().interrupt();
        }
      } catch (MongoInterruptedException e) {
        logger.warn("Mongo master interrupted");
        throw new ShutDownException(e);
      }
    }
  }

  public abstract void closeCursor();

  public abstract void configCursor();

  public abstract void eventLoop();
}
