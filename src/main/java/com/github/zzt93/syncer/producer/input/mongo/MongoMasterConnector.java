package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.common.MongoConnection;
import com.github.zzt93.syncer.config.pipeline.input.Table;
import com.github.zzt93.syncer.producer.dispatch.mongo.MongoDispatcher;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.meta.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.google.common.base.Throwables;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class MongoMasterConnector implements MasterConnector {

  private final Logger logger = LoggerFactory.getLogger(MongoMasterConnector.class);

  private final String identifier;
  private final MongoConnection connection;
  private final ConsumerRegistry registry;
  private MongoCursor<Document> cursor;
  private MongoDispatcher mongoDispatcher;
  private MongoClient client;


  public MongoMasterConnector(MongoConnection connection, ConsumerRegistry registry) throws IOException {
    identifier = connection.initIdentifier();

    this.connection = connection;
    this.registry = registry;
    client = new MongoClient(new MongoClientURI(connection.toConnectionUrl(null)));
    configDispatch(connection, registry);
  }

  private void configDispatch(MongoConnection connection, ConsumerRegistry registry) {
    HashMap<Consumer, ProducerSink> schemaSinkMap = registry
        .outputSink(connection);
    mongoDispatcher = new MongoDispatcher(schemaSinkMap);
  }

  private void configCursor(MongoConnection connection, ConsumerRegistry registry) {
    this.cursor = getReplicaCursor(connection, registry, client);
  }

  private MongoCursor<Document> getReplicaCursor(MongoConnection connection,
      ConsumerRegistry registry, MongoClient client) {
    MongoDatabase db = client.getDatabase("local");
    MongoCollection<Document> coll = db.getCollection("oplog.rs");
    DocTimestamp docTimestamp = registry.votedMongoId(connection);
    Pattern namespaces = getNamespaces(connection, registry);
    Document query = new Document()
        .append("ns", new BasicDBObject("$regex", namespaces))
        // fromMigrate indicates the operation results from a shard re-balancing.
        //.append("fromMigrate", new BasicDBObject("$exists", "false"))
        ;
    if (docTimestamp.getTimestamp() != null) {
      query.append("ts", new BasicDBObject("$gte", docTimestamp.getTimestamp()));
    } else {
      // initial export
      logger.info("Start with initial export, may cost very long time");
      query.append("ts", new BasicDBObject("$gt", new BsonTimestamp()));
    }

    // no need for capped collections:
    // perform a find() on a capped collection with no ordering specified,
    // MongoDB guarantees that the ordering of results is the same as the insertion order.
    // BasicDBObject sort = new BasicDBObject("$natural", 1);

    return coll.find(query)
        .cursorType(CursorType.TailableAwait)
        .oplogReplay(true)
        .iterator();
  }

  private Pattern getNamespaces(MongoConnection connection, ConsumerRegistry registry) {
    StringJoiner joiner = new StringJoiner("|");
    registry.outputSink(connection)
        .keySet().stream().map(Consumer::getSchemas).flatMap(Set::stream).flatMap(s -> {
      List<Table> tables = s.getTables();
      ArrayList<String> res = new ArrayList<>(tables.size());
      for (Table table : tables) {
        res.add("(" + s.getName() + "\\." + table.getName() + ")");
      }
      return res.stream();
    }).forEach(joiner::add);
    return Pattern.compile(joiner.toString());
  }

  @Override
  public void loop() {
    Thread.currentThread().setName(identifier);

    long sleepInSecond = 1;
    while (!Thread.interrupted()) {
      try {
        configCursor(connection, registry);
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

  private void eventLoop() {
    while (cursor.hasNext()) {
      Document d = cursor.next();
      try {
        mongoDispatcher.dispatch(d);
      } catch (InvalidConfigException e) {
        ShutDownCenter.initShutDown();
      } catch (Throwable e) {
        // TODO 18/1/26 how to retry?
        logger.error("Fail to dispatch this doc {}", d, e);
        Throwables.throwIfUnchecked(e);
      }
    }
  }

}
