package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see InsertByQuery
 * @see SyncByQuery
 */
public class SyncByQueryES extends SyncByQuery {

  private static final Logger logger = LoggerFactory.getLogger(SyncByQueryES.class);

  private final HashMap<String, Object> upsert = new HashMap<>();
  private final List<String> update = new ArrayList<>();
  private final SyncData outer;

  public SyncByQueryES(SyncData data) {
    outer = data;
  }

  public SyncByQueryES update(String fieldName) {
    update.add(fieldName);
    outer.setEventType(EventType.UPDATE_ROWS);
    return this;
  }

  List<String> getUpdate() {
    return update;
  }
}
