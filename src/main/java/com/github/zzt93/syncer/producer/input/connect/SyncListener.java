package com.github.zzt93.syncer.producer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {


  private final Logger logger = LoggerFactory.getLogger(SyncListener.class);
  private final Dispatcher dispatcher;
  private Event last;

  public SyncListener(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void onEvent(Event event) {
    EventType eventType = event.getHeader().getEventType();
    switch (eventType) {
      case TABLE_MAP:
        this.last = event;
        break;
      case WRITE_ROWS:
      case UPDATE_ROWS:
      case DELETE_ROWS:
        dispatcher.dispatch(last, event);
        last = null;
        break;
      default:
        logger.trace("Receive binlog event: {}", event.toString());
    }
  }
}
