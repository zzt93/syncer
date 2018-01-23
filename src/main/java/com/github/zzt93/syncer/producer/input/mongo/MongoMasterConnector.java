package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.config.pipeline.InvalidPasswordException;
import com.github.zzt93.syncer.config.pipeline.common.MongoConnection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.config.pipeline.input.Table;
import com.github.zzt93.syncer.producer.dispatch.MongoDispatcher;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class MongoMasterConnector implements MasterConnector {
  private final Logger logger = LoggerFactory.getLogger(MongoMasterConnector.class);

  private final String identifier;
  private final int maxRetry;
  private final AtomicReference<DocTimestamp> docTimestamp = new AtomicReference<>();
  private MongoCursor<Document> cursor;
  private MongoDispatcher mongoDispatcher;


  public MongoMasterConnector(MongoConnection connection, ConsumerRegistry registry,
      int maxRetry) throws IOException {
    identifier = connection.connectionIdentifier();
    this.maxRetry = maxRetry;

    String password = FileUtil.readAll(connection.getPasswordFile());
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    configCursor(connection, registry);
    configDispatch(connection, registry);
  }

  private void configDispatch(MongoConnection connection, ConsumerRegistry registry) {
    IdentityHashMap<Set<Schema>, OutputSink> schemaSinkMap = registry
        .outputSink(connection);
    mongoDispatcher = new MongoDispatcher(schemaSinkMap);
  }

  private void configCursor(MongoConnection connection, ConsumerRegistry registry) {
    MongoClient client = new MongoClient(
        new MongoClientURI(connection.toConnectionUrl(null)));
    MongoDatabase db = client.getDatabase("local");
    MongoCollection<Document> coll = db.getCollection("oplog.rs");
    DocTimestamp docTimestamp = registry.votedMongoId(connection);
    Pattern namespaces = getNamespaces(connection, registry);
    Document query = new Document()
        .append("ts", new BasicDBObject("$gte", docTimestamp.getTimestamp()))
        .append("ns", new BasicDBObject("$in", namespaces))
        // fromMigrate indicates the operation results from a shard rebalancing.
        //.append("fromMigrate", new BasicDBObject("$exists", "false"))
        ;

    // no need for capped collections:
    // perform a find() on a capped collection with no ordering specified,
    // MongoDB guarantees that the ordering of results is the same as the insertion order.
//    BasicDBObject sort = new BasicDBObject("$natural", 1);

    this.cursor = coll.find(query)
        .cursorType(CursorType.TailableAwait)
        .oplogReplay(true)
        .iterator();
  }

  private Pattern getNamespaces(MongoConnection connection, ConsumerRegistry registry) {
    StringJoiner joiner = new StringJoiner("|");
    registry.outputSink(connection)
        .keySet().stream().flatMap(Set::stream).flatMap(s -> {
          List<Table> tables = s.getTables();
          ArrayList<String> res = new ArrayList<>(tables.size());
          for (Table table : tables) {
            res.add("("+s.getName() + "\\." + table.getName()+")");
          }
          return res.stream();
        }).forEach(joiner::add);
    return Pattern.compile(joiner.toString());
  }

  @Override
  public void run() {
    Thread.currentThread().setName(identifier);
    for (int i = 0; i < maxRetry; i++) {
      while (!Thread.interrupted()) {
        while (cursor.hasNext()) {
          Document d = cursor.next();
          mongoDispatcher.dispatch(d);
          docTimestamp.set(new DocTimestamp((BSONTimestamp) d.get("ts")));
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
          logger.error("", ignored);
        }
      }
    }
  }
}
