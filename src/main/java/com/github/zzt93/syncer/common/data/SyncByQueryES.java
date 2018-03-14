package com.github.zzt93.syncer.common.data;

import com.github.shyiko.mysql.binlog.event.EventType;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see InsertByQuery
 * @see SyncByQuery
 */
public class SyncByQueryES extends SyncByQuery {

  private static final Logger logger = LoggerFactory.getLogger(SyncByQueryES.class);

  private final HashMap<String, Object> upsert = new HashMap<>();
  private final HashMap<String, Object> append = new HashMap<>();
  private final HashMap<String, Object> remove = new HashMap<>();
  private transient final SyncData outer;

  public SyncByQueryES(SyncData data) {
    outer = data;
  }

  public SyncByQueryES updateList(String listField, Object delta) {
    switch (outer.getType()) {
      case DELETE_ROWS:
        remove.put(listField, delta);
        break;
      case WRITE_ROWS:
        append.put(listField, delta);
        break;
      default:
        logger.warn("Not support update list variable for {}", outer.getType());
    }
    outer.setEventType(EventType.UPDATE_ROWS);
    return this;
  }

  public HashMap<String, Object> getAppend() {
    return append;
  }

  public HashMap<String, Object> getRemove() {
    return remove;
  }

  @Override
  public String toString() {
    return "SyncByQueryES{" +
        "upsert=" + upsert +
        ", append=" + append +
        ", remove=" + remove +
        ", outer=SyncData@" + Integer.toHexString(outer.hashCode()) +
        '}';
  }
}
