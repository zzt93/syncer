package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.data.SyncData;

import java.util.LinkedList;
import java.util.List;

public interface ConditionalStatement extends CompositeStatement {


  @Override
  default void recurWithSingleElement(LinkedList<SyncData> tmp) {
    List<ExprFilter> code = conditional(tmp.getFirst());
    if (code == null) {
      code = new LinkedList<>();
    }
    for (ExprFilter filter : code) {
      filter.filter(tmp);
    }
  }

  List<ExprFilter> conditional(SyncData syncData);

}
