package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchema;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class MongoDispatcher implements Dispatcher {

  private static final String ID = "_id";
  private final Logger logger = LoggerFactory.getLogger(MongoDispatcher.class);
  private final HashMap<String, List<JsonKeyFilter>> directOutput = new HashMap<>();
  private final HashMap<Pattern, JsonKeyFilter> regexOutput = new HashMap<>();

  public MongoDispatcher(IdentityHashMap<ConsumerSchema, OutputSink> schemaSinkMap) {
    for (Entry<ConsumerSchema, OutputSink> entry : schemaSinkMap.entrySet()) {
      for (Schema schema : entry.getKey().getSchemas()) {
        if (schema.noNamePattern()) {
          directOutput.computeIfAbsent(schema.getName(), k -> new ArrayList<>())
              .add(new JsonKeyFilter(schema, entry.getValue()));
        } else {
          regexOutput.put(schema.getNamePattern(), new JsonKeyFilter(schema, entry.getValue()));
        }
      }
    }
  }

  @Override
  public boolean dispatch(Object... data) {
    Document document = (Document) data[0];
    String eventId = IdGenerator.fromDocument(document);
    MDC.put(IdGenerator.EID, eventId);

    String[] namespace = document.getString("ns").split("\\.");
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
        row.putAll((Map) obj.get("$set"));
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
