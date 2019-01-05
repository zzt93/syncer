package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.data.util.SyncFilter;

import java.util.LinkedList;
import java.util.List;

public interface ConditionalStatement extends CompositeStatement {


  @Override
  default void recurWithSingleElement(LinkedList<SyncData> tmp) {
    List<SyncFilter> code = conditional(tmp.getFirst());
    if (code == null) {
      code = new LinkedList<>();
    }
    for (SyncFilter filter : code) {
      filter.filter(tmp);
    }
  }

  List<SyncFilter> conditional(SyncData syncData);

}
