package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.IdGenerator.Offset;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.CloneConfig;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class Clone implements ExprFilter, IfBodyAction {

  private final FilterActions newObjAction;
  private final FilterActions oldObjAction;
  private final List<Expression> copyValue;

  public Clone(SpelExpressionParser parser,
      CloneConfig cloneConfig) throws NoSuchFieldException {
    SpelExpressionParser parser1 = parser;
    List<String> cp = cloneConfig.getCopyValue();
    copyValue = new ArrayList<>(cp.size());
    for (String s : cp) {
      copyValue.add(parser.parseExpression(s));
    }
    newObjAction = new FilterActions(parser, cloneConfig.getNew());
    oldObjAction = new FilterActions(parser, cloneConfig.getOld());
  }

  @Override
  public Void decide(List<SyncData> dataList) {
    LinkedList<SyncData> list = new LinkedList<>();
    for (SyncData src : dataList) {
      SyncData clone = clone(src);
      list.add(clone);
    }
    dataList.addAll(list);
    return null;
  }

  private SyncData clone(SyncData src) {
    SyncData clone = new SyncData(src, Offset.CLONE.getOffset());
    for (Expression s : copyValue) {
      Object value = s.getValue(src.getContext());
      s.setValue(clone.getContext(), value);
    }
    newObjAction.execute(clone.getContext());
    oldObjAction.execute(src.getContext());
    return clone;
  }

  @Override
  public Object execute(SyncData data) {
    return clone(data);
  }
}
