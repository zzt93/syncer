package com.github.zzt93.syncer.common.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class FullRowUpdateImageMapper implements RowUpdateImageMapper {
  private final Logger logger = LoggerFactory.getLogger(FullRowUpdateImageMapper.class);

  private final BitSet includedColumns;
  private final Set<Integer> primaryKeys;
  private final String table;

  public FullRowUpdateImageMapper(Event tableMap,
      Map<Integer, String> indexToName, Set<Integer> primaryKeys,
      BitSet includedColumns) {
    TableMapEventData data = tableMap.getData();
    this.table = data.getTable();
    this.primaryKeys = primaryKeys;
    this.includedColumns = includedColumns;
  }

  @Override
  public HashMap<Integer, Object> map(Entry<Serializable[], Serializable[]> row) {
    HashMap<Integer, Object> map = new HashMap<>();
    Serializable[] before = row.getKey();
    Serializable[] after = row.getValue();
    Assert.isTrue(before.length == after.length && after.length == includedColumns.length(),
        "before and after row image are not same length");
    // only 'full' now
    for (int i = 0; i < after.length; i++) {
      if (primaryKeys.contains(i) && before[i] != after[i]) {
        logger.warn("Update id of table, some output channel may not support");
        // use primary key before update
        // use i+1 because the index of mysql column is count from 1
        map.put(i + 1, before[i]);
      } else if (before[i] != after[i]) {
        // use i+1 because the index of mysql column is count from 1
        map.put(i + 1, after[i]);
      } else {
        logger.debug("Ignore value{} of table{}", after[i], table);
      }
    }
    return map;
  }
}
