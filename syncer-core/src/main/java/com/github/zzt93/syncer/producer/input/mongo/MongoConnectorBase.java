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
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author zzt
 */
public abstract class MongoConnectorBase implements MasterConnector {
  static final String OPLOG_RS = "oplog.rs";
  static final String LOCAL = "local";
  static final String TS = "ts";
  private final String identifier;
  private final Logger logger = LoggerFactory.getLogger(MongoConnectorBase.class);
  protected MongoClient client;

  MongoConnectorBase(MongoConnection connection) {
    client = new MongoClient(new MongoClientURI(connection.toConnectionUrl(null)));
    identifier = connection.connectionIdentifier();
  }

  static Object mongoMapping(Object o) {
    if (o instanceof Map) {
      if (((Map) o).size() == 1 && ((Map) o).containsKey("$numberDecimal")) {
        return new BigDecimal((String) ((Map) o).get("$numberDecimal"));
      }
      for (Map.Entry<String, Object> e : ((Map<String, Object>) o).entrySet()) {
        e.setValue(mongoMapping(e.getValue()));
      }
    } else if (o instanceof List) {
      List list = (List) o;
      for (int i = 0; i < list.size(); i++) {
        list.set(i, mongoMapping(list.get(i)));
      }
    } else if (o instanceof Binary) {
      return ((Binary) o).getData();
    } else if (o instanceof Decimal128) {
      return ((Decimal128) o).bigDecimalValue();
    } else if (o instanceof BsonTimestamp) {
      return bsonMapping((BsonValue) o);
    } else if (o instanceof ObjectId) {
      return o.toString();
    }
    return o;
  }

  static Object bsonMapping(BsonValue value) {
    switch (value.getBsonType()) {
      case INT64:
        return value.asInt64().getValue();
      case INT32:
        return value.asInt32().getValue();
      case BINARY:
        return value.asBinary().getData();
      case DOUBLE:
        return value.asDouble().getValue();
      case STRING:
        return value.asString().getValue();
      case BOOLEAN:
        return value.asBoolean().getValue();
      case DATE_TIME:
        return new Date(value.asDateTime().getValue());
      case TIMESTAMP:
        BsonTimestamp bsonTimestamp = value.asTimestamp();
        return new Timestamp(bsonTimestamp.getTime() * 1000 + bsonTimestamp.getInc());
      case DECIMAL128:
        return value.asDecimal128().getValue().bigDecimalValue();
      case OBJECT_ID:
        return value.asObjectId().toString();
      case NULL:
        return null;
      case ARRAY:
        List<BsonValue> values = value.asArray().getValues();
        List<Object> list = new ArrayList<>();
        for (BsonValue bsonValue : values) {
          list.add(bsonMapping(bsonValue));
        }
        return list;
      case DOCUMENT:
        HashMap<String, Object> map = new HashMap<>();
        for (Map.Entry<String, BsonValue> o : value.asDocument().entrySet()) {
          map.put(o.getKey(), bsonMapping(o.getValue()));
        }
        return map;
      default:
        return value;
    }
  }

  <T> Stream<T> getNamespaces(MongoConnection connection, ConsumerRegistry registry, Function<String[], T> f) {
    Set<String> producerDbName = new HashSet<>();
    for (String dbName : client.listDatabaseNames()) {
      producerDbName.add(dbName);
    }

    checkOplog(producerDbName);

    Set<Consumer> consumers = registry.outputSink(connection).keySet();
    return consumers.stream().map(Consumer::getRepos).flatMap(Set::stream).flatMap(s -> {
      if (!producerDbName.contains(s.getName())) {
        throw new InvalidConfigException("No such repo(" + s.getName() + ") in " + connection);
      }
      Set<String> producerCollectionName = new HashSet<>();
      for (String collectionName : client.getDatabase(s.getName()).listCollectionNames()) {
        producerCollectionName.add(collectionName);
      }

      List<Entity> entities = s.getEntities();
      ArrayList<T> res = new ArrayList<>(entities.size());
      for (Entity entity : entities) {
        if (!producerCollectionName.contains(entity.getName())) {
          throw new InvalidConfigException("No such collection(" + s.getName() + "." + entity.getName() + ") in " + connection);
        }
        res.add(f.apply(new String[]{s.getName(), entity.getName()}));
      }
      return res.stream();
    });
  }

  private void checkOplog(Set<String> producerDbName) {
    if (!producerDbName.contains(LOCAL)) {
      throw new InvalidConfigException("Replication not detected. Enable by: rs.initiate()");
    }
    HashSet<String> names = new HashSet<>();
    for (String collectionName : client.getDatabase(LOCAL).listCollectionNames()) {
      names.add(collectionName);
    }
    if (!names.contains(OPLOG_RS)) {
      throw new InvalidConfigException("Replication not detected. Enable by: rs.initiate()");
    }
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
        sleepInSecond = FallBackPolicy.POW_2.sleep(sleepInSecond);
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
