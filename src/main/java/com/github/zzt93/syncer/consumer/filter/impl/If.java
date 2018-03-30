package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.IfConfig;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class If implements ExprFilter {

  private final SpelExpressionParser parser;
  private final Condition ifCondition;
  private final List<IfBodyAction> ifAction;
  private final List<IfBodyAction> elseAction;

  public If(SpelExpressionParser parser, IfConfig ifConfig) {
    this.parser = parser;
    ifConfig.checkConfig();
    ifCondition = new Condition(parser, ifConfig.getCondition());
    ifAction = ifConfig.getIfAction(parser);
    elseAction = ifConfig.getElseAction(parser);
  }

  @Override
  public Void decide(List<SyncData> dataList) {
    LinkedList<SyncData> list = new LinkedList<>();
    for (Iterator<SyncData> iterator = dataList.iterator(); iterator.hasNext(); ) {
      SyncData syncData = iterator.next();
      StandardEvaluationContext context = syncData.getContext();
      Boolean conditionRes = ifCondition.execute(context);
      List<IfBodyAction> action;
      if (conditionRes) {
        action = ifAction;
      } else {
        action = elseAction;
      }
      for (IfBodyAction ifStatement : action) {
        Object res = ifStatement.execute(syncData);
        if (res instanceof Filter.FilterRes) {
          switch ((FilterRes)res) {
            case DENY:
              iterator.remove();
              break;
            case ACCEPT:
              break;
          }
        } else if (res instanceof SyncData) { // Clone SyncData
          list.add((SyncData) res);
        } else if (res instanceof List) {
          list.addAll((Collection<? extends SyncData>) res);
        }
      }
    }
    dataList.addAll(list);
    return null;
  }
}
