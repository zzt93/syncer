package com.github.zzt93.syncer.producer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowsEvent;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import com.github.zzt93.syncer.producer.dispatch.InputEnd;
import com.github.zzt93.syncer.producer.input.filter.InputFilter;
import com.github.zzt93.syncer.producer.input.filter.InputPipeHead;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {

  private final List<InputFilter> filters;
  private final InputPipeHead start;
  private final InputEnd end;
  private final Logger logger = LoggerFactory.getLogger(SyncListener.class);
  private final Dispatcher dispatcher;
  private Event last;

  public SyncListener(InputPipeHead inputPipeHead, List<InputFilter> filters, InputEnd inputEnd,
      Dispatcher dispatcher) {
    this.filters = filters;
    start = inputPipeHead;
    end = inputEnd;
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
        RowsEvent aim = start.decide(last, event);
        if (aim == null) { // not interested in this database+table
          return;
        }
        MDC.put(RowsEvent.EID, aim.getEventId());
        logger.debug("Receive binlog event: {}", aim.toString());
        for (InputFilter filter : filters) {
          if (filter.decide(aim) != FilterRes.ACCEPT) { // not interested in unrelated rows
            // discard: not add to queue
            return;
          }
        }
        SyncData[] syncDatas = end.decide(aim);
        if (!dispatcher.dispatch(syncDatas)) {
          logger.error("Fail to put data to 'filter' processor, space unavailable");
        }
        last = null;
        MDC.remove(RowsEvent.EID);
        break;
      default:
        logger.trace("Receive binlog event: {}", event.toString());
    }
  }
}
