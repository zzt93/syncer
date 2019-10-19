package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.filter.ForkStatement;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author zzt
 */
public class Create implements ForkStatement {

  private final Statement newObjAction;
  private final List<Expression> copyValue;

  public Create(SpelExpressionParser parser, List<String> cp, ArrayList<String> single) {
    copyValue = new ArrayList<>(cp.size());
    for (String s : cp) {
      copyValue.add(parser.parseExpression(s));
    }
    newObjAction = new Statement(parser, single);
  }

  @Override
  public void filter(List<SyncData> dataList) {
    LinkedList<SyncData> list = new LinkedList<>();
    for (SyncData src : dataList) {
      list.add(execute(src));
    }
    dataList.addAll(list);
  }

  public SyncData execute(SyncData src) {
    SyncData create = new SyncData(src, MongoDataId.Offset.CLONE.ordinal());
    for (Expression s : copyValue) {
      Object value = s.getValue(src.getContext());
      s.setValue(create.getContext(), value);
    }
    newObjAction.execute(create);
    return create;
  }
}
