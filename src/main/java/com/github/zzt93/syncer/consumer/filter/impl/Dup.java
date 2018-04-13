package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.IdGenerator.Offset;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class Dup implements ExprFilter, IfBodyAction {

  private final List<FilterActions> newObjAction;
  private final List<Expression> copyValue;


  public Dup(SpelExpressionParser parser, List<String> cp,
      ArrayList<List<String>> multiple) {
    copyValue = new ArrayList<>(cp.size());
    for (String s : cp) {
      copyValue.add(parser.parseExpression(s));
    }
    newObjAction = multiple.stream().map(action -> new FilterActions(parser, action)).collect(Collectors.toList());

  }

  @Override
  public Object execute(SyncData src) {
    LinkedList<SyncData> res = new LinkedList<>();
    for (int i = 0; i < newObjAction.size(); i++) {
      FilterActions filterActions = newObjAction.get(i);
      SyncData dup = new SyncData(src, Offset.DUP.ordinal() + i);
      for (Expression s : copyValue) {
        Object value = s.getValue(src.getContext());
        s.setValue(dup.getContext(), value);
      }
      filterActions.execute(dup.getContext());
      res.add(dup);
    }
    return res;
  }

  @Override
  public Void decide(List<SyncData> dataList) {
    for (SyncData syncData : dataList) {
      execute(syncData);
    }
    return null;
  }
}
