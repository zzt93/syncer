package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class MongoDispatcher implements Dispatcher {

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
  public boolean dispatch(SimpleEventType simpleEventType, Object... data) {
    SyncData syncData = (SyncData) data[0];
    if (syncData == null) {
      return false;
    }

    String db = syncData.getRepo();
    if (directOutput.containsKey(db)) {
      for (JsonKeyFilter keyFilter : directOutput.get(db)) {
        keyFilter.output(syncData);
      }
    } else {
      for (Entry<Pattern, JsonKeyFilter> entry : regexOutput.entrySet()) {
        if (entry.getKey().matcher(db).find()) {
          entry.getValue().output(syncData);
        } else {
          logger.warn("Unknown syncData {}", syncData);
        }
      }
    }
    return true;
  }


}
