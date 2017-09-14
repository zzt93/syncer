package com.github.zzt93.syncer.input.listener;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.RowEvent;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.filter.Filter.FilterRes;
import com.github.zzt93.syncer.input.filter.InputEnd;
import com.github.zzt93.syncer.input.filter.InputFilter;
import com.github.zzt93.syncer.input.filter.InputStart;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {

  private final List<InputFilter> filters;
  private final InputStart start;
  private final InputEnd end;
  private final Logger logger = LoggerFactory.getLogger(SyncListener.class);
  private Event last;

  public SyncListener(InputStart inputStart, List<InputFilter> filters, InputEnd inputEnd) {
    this.filters = filters;
    start = inputStart;
    end = inputEnd;
  }

  @Override
  public void onEvent(Event event) {
    logger.debug("Receive binlog event: {}", event.toString());
    EventType eventType = event.getHeader().getEventType();
    switch (eventType) {
      case TABLE_MAP:
        this.last = event;
        break;
      case WRITE_ROWS:
      case UPDATE_ROWS:
      case DELETE_ROWS:
        RowEvent aim = start.decide(event, last);
        for (InputFilter filter : filters) {
          if (filter.decide(aim) != FilterRes.ACCEPT) {
            // discard: not add to queue
            return;
          }
        }
        SyncData syncData = end.decide(aim);
        // TODO 9/14/17 add to queue
        break;
    }
  }
}
