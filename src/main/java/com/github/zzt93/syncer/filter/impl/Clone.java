package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.CloneConfig;
import com.github.zzt93.syncer.filter.ExprFilter;
import java.util.LinkedList;
import java.util.List;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class Clone implements ExprFilter {

  private final FilterActions newObjAction;
  private final FilterActions oldObjAction;
  private final SpelExpressionParser parser;
  private final List<String> copyValue;

  public Clone(SpelExpressionParser parser,
      CloneConfig cloneConfig) throws NoSuchFieldException {
    this.parser = parser;
    copyValue = cloneConfig.getCopyValue();
    newObjAction = new FilterActions(cloneConfig.getNew());
    oldObjAction = new FilterActions(cloneConfig.getOld());
  }

  @Override
  public Void decide(List<SyncData> dataList) {
    LinkedList<SyncData> list = new LinkedList<>();
    for (SyncData src : dataList) {
      SyncData clone = new SyncData(src.getEventId());
      for (String s : copyValue) {
        Object value = parser.parseExpression(s).getValue(src.getContext());
        parser.parseExpression(s).setValue(clone.getContext(), value);
      }
      newObjAction.execute(parser, clone.getContext());
      oldObjAction.execute(parser, src.getContext());
      list.add(clone);
    }
    dataList.addAll(list);
    return null;
  }

}
