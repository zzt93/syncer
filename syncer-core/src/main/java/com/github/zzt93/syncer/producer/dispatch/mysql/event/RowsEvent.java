package com.github.zzt93.syncer.producer.dispatch.mysql.event;


import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.zzt93.syncer.config.common.MismatchedSchemaException;
import com.github.zzt93.syncer.data.SimpleEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * <a href="https://dev.mysql.com/doc/internals/en/binlog-row-image.html">binlog row image
 * format</a>
 *
 * @author zzt
 */
public abstract class RowsEvent {
  private static final Logger logger = LoggerFactory.getLogger(RowsEvent.class);

  public static List<NamedFullRow> getNamedRows(
      List<IndexedFullRow> indexedRow,
      List<Integer> interestedAndPkIndex, Map<Integer, String> indexToName) {
    List<NamedFullRow> res = new ArrayList<>(indexedRow.size());
    for (IndexedFullRow indexedFullRow : indexedRow) {
      try {
        res.add(indexedFullRow.toNamed(interestedAndPkIndex, indexToName));
      } catch (ArrayIndexOutOfBoundsException e) {
        logger.error("Current schema({}) does not match old binlog record({}), fail to parse it. Try to connect to latest binlog.", interestedAndPkIndex, indexedFullRow.length());
        throw new MismatchedSchemaException("Current schema does not match old binlog record, fail to parse it. Try to connect to latest binlog", e);
      }
    }
    return res;
  }

  public static String getPrimaryKey(Map<Integer, String> indexToName, Set<Integer> primaryKeys) {
    Iterator<Integer> iterator = primaryKeys.iterator();
    Integer key = iterator.next();
    return indexToName.get(key);
  }

  public static List<IndexedFullRow> getIndexedRows(SimpleEventType eventType, EventData data,
                                                    Set<Integer> primaryKeys) {
    switch (eventType) {
      case UPDATE:
        return UpdateRowsEvent.getIndexedRows((UpdateRowsEventData) data);
      case WRITE:
        WriteRowsEventData write = (WriteRowsEventData) data;
        return getIndexedRows(write.getRows(), write.getIncludedColumns());
      case DELETE:
        DeleteRowsEventData delete = (DeleteRowsEventData) data;
        return getIndexedRows(delete.getRows(), delete.getIncludedColumns());
      default:
        throw new IllegalArgumentException("Unsupported event type");
    }
  }

  private static List<IndexedFullRow> getIndexedRows(List<Serializable[]> rows,
                                                     BitSet includedColumns) {
    List<IndexedFullRow> res = new LinkedList<>();
    for (Serializable[] row : rows) {
      res.add(new IndexedFullRow(row));
    }
    return res;
  }

}
