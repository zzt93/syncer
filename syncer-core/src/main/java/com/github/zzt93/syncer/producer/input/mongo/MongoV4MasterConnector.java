package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.LogbackLoggingField;
import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MongoConnection;
import com.github.zzt93.syncer.config.producer.ProducerMaster;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mongo.MongoDispatcher;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.*;
import java.util.stream.Collectors;

import static com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector.ID;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author zzt
 */
public class MongoV4MasterConnector extends MongoConnectorBase {

  static final String LONG = "$$long$$";
  static final String DATE = "$$date$$";
  static final Gson gson = new GsonBuilder()
      .setLongSerializationPolicy(LongSerializationPolicy.STRING)
      .create();
  // TODO 2020/4/4 https://docs.mongodb.com/manual/reference/operator/query/type/#document-type-available-types
  static final JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
      .int64Converter((value, writer) -> {
        writer.writeStartObject();
        writer.writeName(LONG);
        writer.writeString(Long.toString(value));
        writer.writeEndObject();
      })
      .dateTimeConverter((value, writer) -> {
        writer.writeStartObject();
        writer.writeName(DATE);
        writer.writeString(Long.toString(value));
        writer.writeEndObject();
      })
      .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
      .build();

  private static final Logger logger = LoggerFactory.getLogger(MongoV4MasterConnector.class);
  private static final String NS = "ns";
  private static final int MONGO_CHANGE_STREAM_BATCH_SIZE = 100;
  private final boolean bsonConversion;

  private MongoCursor<ChangeStreamDocument<Document>> cursor;
  private MongoDispatcher mongoDispatcher;
  private ChangeStreamIterable<Document> changeStreamDocuments;


  MongoV4MasterConnector(MongoConnection connection, ConsumerRegistry registry, ProducerMaster.MongoV4Option mongoV4Option) {
    super(connection);

    configDispatch(connection, registry);
    configQuery(connection, registry, mongoV4Option.isUpdateLookUp());
    bsonConversion = mongoV4Option.isBsonConversion();
  }

  private List<BsonDocument> getNamespaces(MongoConnection connection, ConsumerRegistry registry) {
    return getNamespaces(connection, registry, (repoEntity) -> new BsonDocument("db", new BsonString(repoEntity[0])).append("coll", new BsonString(repoEntity[1])))
        .collect(Collectors.toList());
  }

  private void configQuery(MongoConnection connection, ConsumerRegistry registry, boolean updateLookUp) {
    List<Bson> pipeline = singletonList(Aggregates.match(
        Filters.and(
            Filters.in(NS, getNamespaces(connection, registry)),
            Filters.in("operationType", asList("insert", "delete", "replace", "update")))));
    changeStreamDocuments = client.watch(pipeline).batchSize(MONGO_CHANGE_STREAM_BATCH_SIZE);

    DocTimestamp docTimestamp = registry.votedMongoId(connection);
    if (DocTimestamp.earliest == docTimestamp) {
      MongoCursor<Document> firstLog = client.getDatabase(LOCAL).getCollection(OPLOG_RS).find(new Document()).limit(1).iterator();
      if (firstLog.hasNext()) {
        Document next = firstLog.next();
        logger.info("Connect to earliest oplog time: {}", next.get(TS));
        changeStreamDocuments.startAtOperationTime(((BsonTimestamp) next.get(TS)));
      } else {
        logger.info("Document not found in local.oplog.rs -- is this a new and empty db instance?");
        changeStreamDocuments.startAtOperationTime(docTimestamp.getTimestamp());
      }
    } else {
      /*
      Optional. The starting point for the change stream.
      If the specified starting point is in the past, it must be in the time range of the oplog.
      To check the time range of the oplog, see rs.printReplicationInfo().
       */
      changeStreamDocuments.startAtOperationTime(docTimestamp.getTimestamp());
    }
    // UPDATE_LOOKUP: return the most current majority-committed version of the updated document.
    // i.e. run at different time, will have different fullDocument
    changeStreamDocuments.fullDocument(updateLookUp ? FullDocument.UPDATE_LOOKUP : FullDocument.DEFAULT);
  }


  private void configDispatch(MongoConnection connection, ConsumerRegistry registry) {
    HashMap<Consumer, ProducerSink> schemaSinkMap = registry
        .outputSink(connection);
    mongoDispatcher = new MongoDispatcher(schemaSinkMap);
  }

  @Override
  public void configCursor() {
    this.cursor = changeStreamDocuments.iterator();
  }

  @Override
  public void closeCursor() {
    if (cursor != null) {
      cursor.close();
    }
  }

  @Override
  public void eventLoop() {
    // here hasNext is not blocking, may change to reactive
    // https://mongodb.github.io/mongo-java-driver-reactivestreams/1.13/getting-started/quick-tour-primer/
    while (cursor.hasNext()) {
      ChangeStreamDocument<Document> d = cursor.next();
      MongoDataId dataId = DataId.fromDocument(d.getClusterTime());
      MDC.put(LogbackLoggingField.EID, dataId.eventId());
      try {
        mongoDispatcher.dispatch(null, fromChangeStream(d, dataId));
      } catch (InvalidConfigException e) {
        ShutDownCenter.initShutDown(e);
      } catch (Throwable e) {
        logger.error("Fail to dispatch this doc {}", d);
        Throwables.throwIfUnchecked(e);
      }
    }
  }

  private SyncData fromChangeStream(ChangeStreamDocument<Document> d, MongoDataId dataId) {
    MongoNamespace namespace = d.getNamespace();
    HashMap<String, Object> full = new HashMap<>(), updated = null;
    SimpleEventType type;
    switch (d.getOperationType()) {
      case UPDATE:
        // e.g. members.6.state -> {BsonInt32@9194} "BsonInt32{value=1}"
        // e.g. addToSet  -> all elements in bson array
        type = SimpleEventType.UPDATE;
        assert d.getUpdateDescription() != null;

        updated = new HashMap<>(getUpdatedFields(d));
        if (d.getFullDocument() != null) {
          full.putAll(getFullDocument(d));
          // use UpdateDescription to overrides latest version
        } else {
          full.put(ID, getId(d));
        }
        full.putAll(updated);
        addRemovedFields(updated, d.getUpdateDescription().getRemovedFields());
        break;
      case DELETE:
        type = SimpleEventType.DELETE;
        full.put(ID, getId(d));
        break;
      case INSERT:
      case REPLACE: // write will overwrite for ES, not suitable for other output
        type = SimpleEventType.WRITE;
        full.putAll(getFullDocument(d));
        break;
      case OTHER:
      case INVALIDATE:
      case RENAME:
      case DROP:
      case DROP_DATABASE:
      default:
        return null;
    }
    return new SyncData(dataId, type, namespace.getDatabaseName(), namespace.getCollectionName(), ID, full.get(ID), new NamedChangeStream(full, updated));
  }

  private Object getId(ChangeStreamDocument<Document> d) {
    BsonDocument documentKey = d.getDocumentKey();
    return getId(documentKey);
  }

  static Object getId(BsonDocument documentKey) {
    Object o = gson.fromJson(documentKey.toJson(jsonWriterSettings), Map.class).get(ID);
    if (o instanceof Map && ((Map) o).containsKey(LONG)) {
      return Long.parseLong((String) ((Map) o).get(LONG));
    }
    return o;
  }

  private Document getFullDocument(ChangeStreamDocument<Document> d) {
    return d.getFullDocument(); // value in Document is all java type, no need to do bson conversion
  }

  private Map getUpdatedFields(ChangeStreamDocument<Document> changeStreamDocument) {
    UpdateDescription updateDescription = changeStreamDocument.getUpdateDescription();
    if (bsonConversion) {
      Document fullDocument = changeStreamDocument.getFullDocument();
      if (fullDocument == null) {
        Map updated = gson.fromJson(updateDescription.getUpdatedFields().toJson(jsonWriterSettings), Map.class);
        return (Map) parseBson(updated);
      }
      HashMap<String, Object> res = new HashMap<>();
      for (String key : updateDescription.getUpdatedFields().keySet()) {
        res.put(key, fullDocument.get(key));
      }
      return res;
    }
    return updateDescription.getUpdatedFields();
  }

  static Object parseBson(Map<String, Object> map) {
    Set<Map.Entry<String, Object>> set = map.entrySet();
    for (Map.Entry<String, Object> o : set) {
      if (set.size() == 1 && o.getKey().equals(LONG)) {
        return Long.parseLong((String) o.getValue());
      } else if (set.size() == 1 && o.getKey().equals(DATE)) {
        return new Date(Long.parseLong((String) o.getValue()));
      } else if (o.getValue() instanceof Map) {
        o.setValue(parseBson((Map<String, Object>) o.getValue()));
      }
    }
    return map;
  }

  private void addRemovedFields(HashMap<String, Object> updated, List<String> removedFields) {
    if (removedFields == null) {
      return;
    }
    for (String removedField : removedFields) {
      updated.put(removedField, null);
    }
  }

}
