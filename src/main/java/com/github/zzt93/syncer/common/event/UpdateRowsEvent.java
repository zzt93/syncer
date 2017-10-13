package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Created by zzt on 9/14/17. <p> <h3> <a href="https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html">The
 * format of update rows log event</a></h3>
 *
 * <ul> <li>before update row image & bit field indicating presence</li> <li>after update row image
 * & bit field indicating presence</li> </ul>
 *
 * <p> <a href="https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html">The
 * binlog row image format</a>: For now, only support 'full' </p>
 */
public class UpdateRowsEvent extends RowsEvent {

  private final Logger logger = LoggerFactory.getLogger(UpdateRowsEvent.class);

  public UpdateRowsEvent(Event tableMap, UpdateRowsEventData updateRowsEventData,
      Map<Integer, String> indexToName, Set<Integer> primaryKeys) {
    super(tableMap, indexToName,primaryKeys);
    BitSet includedColumns = updateRowsEventData.getIncludedColumns();
    List<Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
    for (Entry<Serializable[], Serializable[]> row : rows) {
      HashMap<Integer, Object> map = new HashMap<>();
      Serializable[] before = row.getKey();
      Serializable[] after = row.getValue();
      // TODO 17/10/10 may support different binlog row image, only 'full' now
      Assert.isTrue(before.length == after.length && after.length == includedColumns.length(),
          "before and after row image are not same length");
      // only 'full' now
      for (int i = 0; i < after.length; i++) {
        if (primaryKeys.contains(i) && before[i] != after[i]) {
          logger.warn("Update id of table, some output channel may not support");
          // use primary key before update
          map.put(i + 1, before[i]);
        } else if (includedColumns.get(i)) {
          // column index start from 1, so +1 here
          map.put(i + 1, after[i]);
        }
      }
      addRow(map);
    }
  }

  @Override
  public EventType operationType() {
    return EventType.UPDATE_ROWS;
  }
}
