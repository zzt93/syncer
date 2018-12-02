package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Repo;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.meta.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.google.common.base.Preconditions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class MongoDispatcher implements Dispatcher {

  private static final String ID = "_id";
  private final Logger logger = LoggerFactory.getLogger(MongoDispatcher.class);
  private final HashMap<String, List<JsonKeyFilter>> directOutput = new HashMap<>();
  private final HashMap<Pattern, JsonKeyFilter> regexOutput = new HashMap<>();

  public MongoDispatcher(HashMap<Consumer, ProducerSink> schemaSinkMap) {
    for (Entry<Consumer, ProducerSink> entry : schemaSinkMap.entrySet()) {
      for (Repo repo : entry.getKey().getRepos()) {
        if (repo.noNamePattern()) {
          directOutput.computeIfAbsent(repo.getName(), k -> new ArrayList<>())
              .add(new JsonKeyFilter(repo, entry.getValue()));
        } else {
          regexOutput.put(repo.getNamePattern(), new JsonKeyFilter(repo, entry.getValue()));
        }
      }
    }
  }

  @Override
  public boolean dispatch(Object... data) {
    Document document = (Document) data[0];
    String eventId = IdGenerator.fromDocument(document);
    MDC.put(IdGenerator.EID, eventId);

    String[] namespace = document.getString(MongoMasterConnector.NS).split("\\.");
    SyncData syncData = fromDocument(document, eventId, namespace);
    if (syncData == null) {
      return false;
    }

    String db = namespace[0];
    if (directOutput.containsKey(db)) {
      for (JsonKeyFilter keyFilter : directOutput.get(db)) {
        keyFilter.output(syncData);
      }
    } else {
      for (Entry<Pattern, JsonKeyFilter> entry : regexOutput.entrySet()) {
        if (entry.getKey().matcher(db).find()) {
          entry.getValue().output(syncData);
        } else {
          logger.warn("Unknown document {}", document);
        }
      }
    }
    return true;
  }

  /**
   * <ul> <li> {"ts":Timestamp(1521530692,1),"t":NumberLong("5"),"h":NumberLong("-384939294837368966"),
   * "v":2,"op":"u","ns":"foo.bar","o2":{"_id":"L0KB$fjfLFra"},"o":{"$set":{"apns":"[]"}}} </li>
   * </ul>
   */
  private SyncData fromDocument(Document document, String eventId, String[] namespace) {
    String op = document.getString("op");
    Map<String, Object> row = new HashMap<>();
    EventType type;
    Map obj = (Map) document.get("o");
    switch (op) {
      case "u":
        type = EventType.UPDATE_ROWS;
        // see issue for format explanation: https://jira.mongodb.org/browse/SERVER-37306
        row.putAll((Map) obj.getOrDefault("$set", obj));
        row.putAll((Map) document.get("o2"));
        break;
      case "i":
        type = EventType.WRITE_ROWS;
        row.putAll(obj);
        break;
      case "d":
        type = EventType.DELETE_ROWS;
        row.putAll(obj);
        break;
      default:
        return null;
    }
    Preconditions.checkState(row.containsKey("_id"));
    return new SyncData(eventId, 0, namespace[0], namespace[1], ID, row.get(ID), row, type);
  }
}
