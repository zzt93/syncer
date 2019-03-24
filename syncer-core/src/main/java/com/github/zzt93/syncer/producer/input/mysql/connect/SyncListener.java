package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.MysqlDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {


  /**
   * @see com.github.zzt93.syncer.producer.input.mysql.connect.SyncListener#onEvent(Event)
   */
  private static Map<EventType, SimpleEventType> map = new EnumMap<>(EventType.class);

  static {
    // unify multiple update/delete/write into one single type
    // to avoid checking wrong type
    map.put(EventType.PRE_GA_WRITE_ROWS, SimpleEventType.WRITE);
    map.put(EventType.WRITE_ROWS, SimpleEventType.WRITE);
    map.put(EventType.EXT_WRITE_ROWS, SimpleEventType.WRITE);

    map.put(EventType.PRE_GA_UPDATE_ROWS, SimpleEventType.UPDATE);
    map.put(EventType.UPDATE_ROWS, SimpleEventType.UPDATE);
    map.put(EventType.EXT_UPDATE_ROWS, SimpleEventType.UPDATE);

    map.put(EventType.PRE_GA_DELETE_ROWS, SimpleEventType.DELETE);
    map.put(EventType.DELETE_ROWS, SimpleEventType.DELETE);
    map.put(EventType.EXT_DELETE_ROWS, SimpleEventType.DELETE);
  }

  private final Logger logger = LoggerFactory.getLogger(SyncListener.class);
  private final MysqlDispatcher mysqlDispatcher;
  private Event last;

  public SyncListener(MysqlDispatcher mysqlDispatcher) {
    this.mysqlDispatcher = mysqlDispatcher;
  }

  /**
   * May return null, should be handled
   *
   * @see #onEvent(Event)
   */
  private static SimpleEventType toSimpleEvent(EventType type) {
    return map.getOrDefault(type, null);
  }

  @Override
  public void onEvent(Event event) {
    EventType eventType = event.getHeader().getEventType();
    logger.trace("Receive binlog event: {}", event);
    switch (eventType) {
      case TABLE_MAP:
        this.last = event;
        break;
      default:
        SimpleEventType type = toSimpleEvent(eventType);
        if (type == null) {
          break;
        }
        try {
          mysqlDispatcher.dispatch(type, last, event);
        } catch (InvalidConfigException e) {
          ShutDownCenter.initShutDown(e);
        } catch (Throwable e) {
          logger.error("Fail to dispatch {}", event);
          ShutDownCenter.initShutDown(e);
        }
        // TODO 2019/3/15 alter table event, current position event
    }
  }

}
