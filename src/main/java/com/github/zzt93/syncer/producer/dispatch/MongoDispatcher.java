package com.github.zzt93.syncer.producer.dispatch;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class MongoDispatcher implements Dispatcher {

  private final Logger logger = LoggerFactory.getLogger(MongoDispatcher.class);
  private final HashMap<String, List<OutputSink>> directOutput = new HashMap<>();
  private final HashMap<Pattern, OutputSink> regexOutput = new HashMap<>();

  public MongoDispatcher(IdentityHashMap<Set<Schema>, OutputSink> schemaSinkMap) {
    for (Entry<Set<Schema>, OutputSink> entry : schemaSinkMap.entrySet()) {
      for (Schema schema : entry.getKey()) {
        if (!schema.hasNamePattern()) {
          directOutput.computeIfAbsent(schema.getName(), k -> new ArrayList<>())
              .add(entry.getValue());
        } else {
          regexOutput.put(schema.getNamePattern(), entry.getValue());
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

    String db = namespace[0], collection = namespace[1];
    if (directOutput.containsKey(db)) {
      for (OutputSink outputSink : directOutput.get(db)) {
        outputSink.output(syncData);
      }
    } else {
      for (Entry<Pattern, OutputSink> entry : regexOutput.entrySet()) {
        if (entry.getKey().matcher(db).find()) {
          entry.getValue().output(syncData);
        } else {
          logger.warn("Unknown document {}", document);
        }
      }
    }
    return true;
  }

  private SyncData fromDocument(Document document, String eventId, String[] namespace) {
    String op = document.getString("op");
    Map<String, Object> row = new HashMap<>();
    EventType type = null;
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
    return new SyncData(eventId, 0, namespace[0], namespace[1], "_id", row, type);
  }
}
