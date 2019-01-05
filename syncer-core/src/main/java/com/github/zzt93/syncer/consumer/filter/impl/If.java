package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.consumer.filter.IfConfig;
import com.github.zzt93.syncer.consumer.filter.ConditionalStatement;
import com.github.zzt93.syncer.data.util.SyncFilter;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.List;

/**
 * @author zzt
 */
public class If implements ConditionalStatement {

  private final Condition ifCondition;
  private final List<SyncFilter> ifAction;
  private final List<SyncFilter> elseAction;

  public If(SpelExpressionParser parser, IfConfig ifConfig) {
    ifConfig.checkConfig();
    ifCondition = new Condition(parser, ifConfig.getCondition());
    ifAction = ifConfig.getIfAction(parser);
    elseAction = ifConfig.getElseAction(parser);
  }

  /**
   * @see ConditionalStatement#filter(List)
   */
  @Override
  public List<SyncFilter> conditional(SyncData syncData) {
    StandardEvaluationContext context = syncData.getContext();
    Boolean conditionRes = ifCondition.execute(context);
    List<SyncFilter> action;
    if (conditionRes) {
      action = ifAction;
    } else {
      action = elseAction;
    }
    return action;
  }

}
