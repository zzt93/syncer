package com.github.zzt93.syncer.producer.input.mysql.connect;

import static com.github.shyiko.mysql.binlog.event.EventType.DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.UPDATE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.isDelete;
import static com.github.shyiko.mysql.binlog.event.EventType.isUpdate;
import static com.github.shyiko.mysql.binlog.event.EventType.isWrite;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.producer.dispatch.mysql.MysqlDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {


  private final Logger logger = LoggerFactory.getLogger(SyncListener.class);
  private final MysqlDispatcher mysqlDispatcher;
  private Event last;

  public SyncListener(MysqlDispatcher mysqlDispatcher) {
    this.mysqlDispatcher = mysqlDispatcher;
  }

  @Override
  public void onEvent(Event event) {
    EventType eventType = event.getHeader().getEventType();
    switch (eventType) {
      case TABLE_MAP:
        this.last = event;
        break;
      default:
        if (EventType.isRowMutation(eventType)) {
          try {
            EventHeaderV4 header = event.getHeader();
            // unify multiple update/delete/write into one single type
            // to avoid checking wrong type
            if (isUpdate(eventType)) {
              header.setEventType(UPDATE_ROWS);
            } else if (isWrite(eventType)) {
              header.setEventType(WRITE_ROWS);
            } else if (isDelete(eventType)) {
              header.setEventType(DELETE_ROWS);
            }
            mysqlDispatcher.dispatch(last, event);
          } catch (Exception e) {
            logger.error("", e);
          }
        }
        logger.trace("Receive binlog event: {}", event);
    }
  }
}
