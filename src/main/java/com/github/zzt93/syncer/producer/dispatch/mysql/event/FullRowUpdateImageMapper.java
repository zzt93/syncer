package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
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

  FullRowUpdateImageMapper(Set<Integer> primaryKeys,
      BitSet includedColumns) {
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
    for (int i = 0; i < after.length; i++) {
      // because format 'full', shouldn't be null
      assert after[i] != null && before[i] != null;

      if (primaryKeys.contains(i))  {
        if (!after[i].equals(before[i])) {
          logger.warn("Updating id of table, not supported");
        }
        // use primary key before update
        map.put(i, before[i]);
      } else {
        map.put(i, after[i]);
      }
    }
    return map;
  }
}
