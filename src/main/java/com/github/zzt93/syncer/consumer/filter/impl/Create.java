package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.IdGenerator.Offset;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class Create implements ExprFilter, IfBodyAction {

  private final FilterActions newObjAction;
  private final List<Expression> copyValue;

  public Create(SpelExpressionParser parser, List<String> cp, ArrayList<String> single) {
    copyValue = new ArrayList<>(cp.size());
    for (String s : cp) {
      copyValue.add(parser.parseExpression(s));
    }
    newObjAction = new FilterActions(parser, single);
  }

  @Override
  public Void decide(List<SyncData> dataList) {
    LinkedList<SyncData> list = new LinkedList<>();
    for (SyncData src : dataList) {
      list.add((SyncData) execute(src));
    }
    dataList.addAll(list);
    return null;
  }

  @Override
  public Object execute(SyncData src) {
    SyncData create = new SyncData(src, Offset.CLONE.ordinal());
    for (Expression s : copyValue) {
      Object value = s.getValue(src.getContext());
      s.setValue(create.getContext(), value);
    }
    newObjAction.execute(create.getContext());
    return create;
  }
}
