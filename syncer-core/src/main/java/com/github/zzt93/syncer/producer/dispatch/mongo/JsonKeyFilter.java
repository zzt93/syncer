package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.input.Repo;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.output.ProducerSink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author zzt
 */
public class JsonKeyFilter {

  private final Repo repo;
  private final ProducerSink producerSink;

  public JsonKeyFilter(Repo repo, ProducerSink producerSink) {
    this.repo = repo;
    this.producerSink = producerSink;
  }

  public boolean output(SyncData data) {
    Set<String> tableRow = repo.getTableRow(data.getRepo(), data.getEntity());
    if (tableRow == null) {
      return false;
    }
    HashMap<String, Object> fields = data.getFields();
    HashSet<String> tmp = new HashSet<>();
    for (Entry<String, Object> entry : fields.entrySet()) {
      if (!tableRow.contains(entry.getKey())) {
        tmp.add(entry.getKey());
      }
    }
    tmp.forEach(fields::remove);
    if (fields.isEmpty() && data.getType() == SimpleEventType.UPDATE) {
      return false;
    }
    return producerSink.output(data);
  }
}
