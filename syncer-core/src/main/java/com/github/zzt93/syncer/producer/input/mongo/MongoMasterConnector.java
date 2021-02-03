package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.LogbackLoggingField;
import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.util.MongoTypeUtil;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MongoConnection;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mongo.MongoDispatcher;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class MongoMasterConnector extends MongoConnectorBase {

  static final String ID = "_id";
  private static final String NS = "ns";
  private final Logger logger = LoggerFactory.getLogger(MongoMasterConnector.class);

  private MongoCursor<Document> cursor;
  private MongoDispatcher mongoDispatcher;
  private MongoClient client;
  private Document query;


  MongoMasterConnector(MongoConnection connection, ConsumerRegistry registry) {
    super(connection);

    client = new MongoClient(new MongoClientURI(connection.toConnectionUrl(null)));
    configDispatch(connection, registry);
    configQuery(connection, registry);
  }

  private Pattern getNamespaces(MongoConnection connection, ConsumerRegistry registry) {
    StringJoiner joiner = new StringJoiner("|");
    getNamespaces(connection, registry, (repoEntity) -> "(" + repoEntity[0] + "\\." + repoEntity[1] + ")").forEach(joiner::add);
    return Pattern.compile(joiner.toString());
  }

  private void configQuery(MongoConnection connection, ConsumerRegistry registry) {
    DocTimestamp docTimestamp = registry.votedMongoId(connection);
    Pattern namespaces = getNamespaces(connection, registry);
    query = new Document()
        .append(NS, new BasicDBObject("$regex", namespaces))
        // https://www.mongodb.com/blog/post/tailing-mongodb-oplog-sharded-clusters
        // fromMigrate indicates the operation results from a shard re-balancing.
        .append("fromMigrate", new BasicDBObject("$exists", false))
    ;
    query.append(TS, new BasicDBObject("$gte", docTimestamp.getTimestamp()));
    // no need for capped collections:
    // perform a find() on a capped collection with no ordering specified,
    // MongoDB guarantees that the ordering of results is the same as the insertion order.
    // BasicDBObject sort = new BasicDBObject("$natural", 1);
    logger.debug("Query: {}", query.toJson());
  }

  private void configDispatch(MongoConnection connection, ConsumerRegistry registry) {
    HashMap<Consumer, ProducerSink> schemaSinkMap = registry
        .outputSink(connection);
    mongoDispatcher = new MongoDispatcher(schemaSinkMap);
  }

  public void configCursor() {
    this.cursor = getReplicaCursor(client, query);
  }

  @Override
  public void connectToEarliest(long offset) {

  }

  private MongoCursor<Document> getReplicaCursor(MongoClient client, Document query) {
    MongoDatabase db = client.getDatabase(LOCAL);
    MongoCollection<Document> coll = db.getCollection(OPLOG_RS);

    return coll.find(query)
        .cursorType(CursorType.TailableAwait)
        .oplogReplay(true)
        .iterator();
  }

  @Override
  public void closeCursor() {
    if (cursor != null) {
      cursor.close();
    }
  }

  public void eventLoop() {
    while (cursor.hasNext()) {
      Document d = cursor.next();
      MongoDataId dataId = DataId.fromDocument((BsonTimestamp) d.get(TS));
      MDC.put(LogbackLoggingField.EID, dataId.eventId());

      SyncData syncData = fromDocument(d, dataId);
      try {
        mongoDispatcher.dispatch(null, syncData);
      } catch (InvalidConfigException e) {
        ShutDownCenter.initShutDown(e);
      } catch (Throwable e) {
        logger.error("Fail to dispatch this doc {}", d);
        Throwables.throwIfUnchecked(e);
      }
    }
  }

  /**
   * <pre>
   *   {"ts":Timestamp(1521530692,1),"t":NumberLong("5"),"h":NumberLong("-384939294837368966"),
   *    * "v":2,"op":"u","ns":"foo.bar","o2":{"_id":"L0KB$fjfLFra"},"o":{"$set":{"apns":"[]"}}}
   * </pre>
   * <pre>
   *   {
   * 	"ts" : Timestamp(1557198762, 1),
   * 	"t" : NumberLong("2"),
   * 	"h" : NumberLong("945776108160130856"),
   * 	"v" : 2,
   * 	"op" : "u",
   * 	"ns" : "audit.audit_record",
   * 	"o2" : {
   * 		"_id" : NumberLong("12241894")
   *    },
   * 	"o" : {
   * 		"$set" : {
   * 			"criteriaAuditRecords.0.state" : 0,
   * 			"criteriaAuditRecords.0.auditTime" : NumberLong("1557198762937"),
   * 			"criteriaAuditRecords.0.auditRoleId" : NumberLong("13041193"),
   * 			"remainCriteriaCount" : 0
   *    }
   *  }
   * }
   * </pre>
   */
  private SyncData fromDocument(Document document, MongoDataId dataId) {
    String[] namespace = document.getString(MongoMasterConnector.NS).split("\\.");

    String op = document.getString("op");
    HashMap<String, Object> row = new HashMap<>();
    SimpleEventType type;
    Map obj = (Map) document.get("o");
    switch (op) {
      case "u":
        type = SimpleEventType.UPDATE;
        // see issue for format explanation: https://jira.mongodb.org/browse/SERVER-37306
        row.putAll((Map) obj.getOrDefault("$set", obj));
        row.putAll((Map) document.get("o2"));
        break;
      case "i":
        type = SimpleEventType.WRITE;
        row.putAll(obj);
        break;
      case "d":
        type = SimpleEventType.DELETE;
        row.putAll(obj);
        break;
      default:
        return null;
    }
    Preconditions.checkState(row.containsKey(ID));
    row = (HashMap<String, Object>) MongoTypeUtil.convertBsonTypes(row);
    return new SyncData(dataId, type, namespace[0], namespace[1], ID, row.get(ID), new NamedUpdatedDoc(row));
  }


}
