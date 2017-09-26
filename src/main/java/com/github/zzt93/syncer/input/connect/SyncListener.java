package com.github.zzt93.syncer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowsEvent;
import com.github.zzt93.syncer.input.filter.InputEnd;
import com.github.zzt93.syncer.input.filter.InputFilter;
import com.github.zzt93.syncer.input.filter.InputStart;
import java.util.List;
import java.util.concurrent.BlockingQueue;
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
  private final BlockingQueue<SyncData> toFilter;
  private Event last;

  public SyncListener(InputStart inputStart, List<InputFilter> filters, InputEnd inputEnd,
      BlockingQueue<SyncData> queue) {
    this.filters = filters;
    start = inputStart;
    end = inputEnd;
    toFilter = queue;
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
        RowsEvent aim = start.decide(last, event);
        if (aim == null) { // not interested in this database+table
          return;
        }
        for (InputFilter filter : filters) {
          if (filter.decide(aim) != FilterRes.ACCEPT) { // not interested in unrelated rows
            // discard: not add to queue
            return;
          }
        }
        SyncData[] syncDatas = end.decide(aim);
        for (SyncData syncData : syncDatas) {
          if (!toFilter.offer(syncData)) {
            logger.error("Fail to put data to 'filter' processor, space unavailable");
          }
        }
        last = null;
        break;
    }
  }
}
