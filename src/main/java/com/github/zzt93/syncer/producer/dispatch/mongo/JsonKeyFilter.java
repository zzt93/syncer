package com.github.zzt93.syncer.producer.dispatch.mongo;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author zzt
 */
public class JsonKeyFilter {

  private final Schema schema;
  private final OutputSink outputSink;

  public JsonKeyFilter(Schema schema, OutputSink outputSink) {
    this.schema = schema;
    this.outputSink = outputSink;
  }

  public boolean output(SyncData data) {
    Set<String> tableRow = schema.getTableRow(data.getSchema(), data.getTable());
    HashMap<String, Object> records = data.getRecords();
    HashSet<String> tmp = new HashSet<>();
    for (Entry<String, Object> entry : records.entrySet()) {
      if (!tableRow.contains(entry.getKey())) {
        tmp.add(entry.getKey());
      }
    }
    tmp.forEach(records::remove);
    return outputSink.output(data);
  }
}
