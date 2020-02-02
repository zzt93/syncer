package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.LogbackLoggingField;
import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MongoConnection;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mongo.MongoDispatcher;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.google.common.base.Throwables;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import static com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector.ID;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author zzt
 */
public class MongoV4MasterConnector extends MongoConnectorBase {

  private final Logger logger = LoggerFactory.getLogger(MongoV4MasterConnector.class);
  private static final String NS = "ns";
  private static final int MONGO_CHANGE_STREAM_BATCH_SIZE = 100;

  private MongoCursor<ChangeStreamDocument<Document>> cursor;
  private MongoDispatcher mongoDispatcher;
  private ChangeStreamIterable<Document> changeStreamDocuments;


  MongoV4MasterConnector(MongoConnection connection, ConsumerRegistry registry) {
    super(connection);

    configDispatch(connection, registry);
    configQuery(connection, registry);
  }

  private void configQuery(MongoConnection connection, ConsumerRegistry registry) {
    DocTimestamp docTimestamp = registry.votedMongoId(connection);
    Pattern namespaces = getNamespaces(connection, registry);

    List<Bson> pipeline = singletonList(Aggregates.match(Filters.and(
        Filters.regex(NS, namespaces), Filters.in("operationType", asList("insert", "delete", "replace", "update")))));
    changeStreamDocuments = client.watch(pipeline).batchSize(MONGO_CHANGE_STREAM_BATCH_SIZE)
        .fullDocument(FullDocument.DEFAULT).startAtOperationTime(docTimestamp.getTimestamp());
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
    full.put(ID, d.getDocumentKey().get(ID));
    SimpleEventType type;
    switch (d.getOperationType()) {
      case UPDATE:
        type = SimpleEventType.UPDATE;
        UpdateDescription updateDescription = d.getUpdateDescription();
        updated = new HashMap<>(updateDescription.getUpdatedFields());
        removeField(updated, updateDescription.getRemovedFields());
        if (d.getFullDocument() != null) {
          full.putAll(d.getFullDocument());
        }
        break;
      case DELETE:
        type = SimpleEventType.DELETE;
        break;
      case INSERT:
      case REPLACE: // write will overwrite for ES, not suitable for other output
        type = SimpleEventType.WRITE;
        full.putAll(d.getFullDocument());
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

  private void removeField(HashMap<String, Object> updated, List<String> removedFields) {
    if (removedFields == null) {
      return;
    }
    for (String removedField : removedFields) {
      updated.put(removedField, null);
    }
  }

}
