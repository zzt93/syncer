package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author zzt
 */
public class JsonKeyFilter {

  private final Repo repo;
  private final ProducerSink producerSink;
  private final static Logger logger = LoggerFactory.getLogger(JsonKeyFilter.class);

  public JsonKeyFilter(Repo repo, ProducerSink producerSink) {
    this.repo = repo;
    this.producerSink = producerSink;
  }

  public boolean output(SyncData from) {
    Set<String> tableRow = repo.getTableRow(from.getRepo(), from.getEntity());
    if (tableRow == null) {
      return false;
    }
    SyncData data = from.copy();
    HashMap<String, Object> fields = data.getFields();
    for(Iterator<Entry<String, Object>> it = fields.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, Object> entry = it.next();
      if (!tableRow.contains(entry.getKey()) && !tableRow.contains(entry.getKey().split("\\.")[0])) {
        it.remove();
        if (data.getUpdated() != null) {
          data.getUpdated().remove(entry.getKey());
        }
      }
    }
    if (data.getType() == SimpleEventType.UPDATE && data.getUpdated().isEmpty()) {
      logger.debug("Discard {} because nothing to update", data);
      return false;
    }
    return producerSink.output(data);
  }
}
