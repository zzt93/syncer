package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.event.RowsEvent;
import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public class InputEnd implements Filter<RowsEvent, SyncData[]> {

  @Override
  public SyncData[] decide(RowsEvent e) {
    List<HashMap<String, Object>> data = e.getData();
    SyncData[] res = new SyncData[data.size()];
    for (int i = 0; i < res.length; i++) {
      res[i] = new SyncData(e.getTableMap(), data.get(i), e.type());
    }
    return res;
  }
}
