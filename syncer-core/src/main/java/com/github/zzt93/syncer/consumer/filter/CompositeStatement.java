package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;

import java.util.LinkedList;
import java.util.List;

public interface CompositeStatement extends ExprFilter {



  @ThreadSafe(safe = {ExprFilter.class})
  @Override
  default void filter(List<SyncData> data) {
    LinkedList<SyncData> res = new LinkedList<>();
    for (SyncData syncData : data) {
      LinkedList<SyncData> tmp = new LinkedList<>();
      tmp.add(syncData);

      recurWithSingleElement(tmp);

      res.addAll(tmp);
    }
    data.clear();
    data.addAll(res);
  }

  /**
   * Filter with inner nested statement/filter one by one
   * @param tmp Precondition: size == 1
   */
  void recurWithSingleElement(LinkedList<SyncData> tmp);

}
