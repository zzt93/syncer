package com.github.zzt93.syncer.config.code;

import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class CheckMongoType implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    if (sync.containField("simples")) {
      for (Map simple : ((List<Map>) sync.getField("simples"))) {
        Long id = (Long) simple.get("id");
        Integer tinyint = (Integer) simple.get("tinyint");
        Long bigint = (Long) simple.get("bigint");
        byte[] bytes = (byte[]) simple.get("bytes");
        if (bytes != null) {
          simple.put("bytes", new String(bytes));
        }
        String varchar = (String) simple.get("varchar");
        BigDecimal decimal = (BigDecimal) simple.get("decimal");
        Double aDouble = (Double) simple.get("aDouble");
        org.bson.BsonTimestamp timestamp = (org.bson.BsonTimestamp) simple.get("timestamp");
      }
    }
    if (sync.containField("nestedIn")) {
      Map nestedIn = (Map) sync.getField("nestedIn");
      Long id = (Long) nestedIn.get("id");
      java.util.Date time = (java.util.Date) nestedIn.get("time");
      if (time != null) {
        nestedIn.put("time", new Timestamp(time.getTime()));
      }
      String currency = (String) nestedIn.get("currency");
      String total = (String) nestedIn.get("total");
      Integer quantity = (Integer) nestedIn.get("quantity");
      Integer type = (Integer) nestedIn.get("type");
      String name = (String) nestedIn.get("name");
      String unit = (String) nestedIn.get("unit");
    }
    sync.es(sync.getRepo() + "-multi", sync.getEntity());
  }

}
