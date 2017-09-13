package com.github.zzt93.syncer.input.listener;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.filter.Filter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {

  private final List<Filter> filters;
  private Logger logger = LoggerFactory.getLogger(SyncListener.class);
  private Event last;

  public SyncListener(List<Filter> filters) {
    this.filters = filters;
  }

  @Override
  public void onEvent(Event event) {
    logger.debug(event.toString());
    EventType eventType = event.getHeader().getEventType();
    switch (eventType) {
      case TABLE_MAP:
        this.last = event;
        break;
      case WRITE_ROWS:
        break;
      case UPDATE_ROWS:
        break;
      case DELETE_ROWS:
        break;
    }
  }
}
