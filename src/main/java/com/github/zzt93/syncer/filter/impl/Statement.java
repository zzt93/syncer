package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.filter.ExprFilter;
import java.util.List;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class Statement implements ExprFilter {

  private final SpelExpressionParser parser;
  private final FilterActions filterActions;

  public Statement(SpelExpressionParser parser, List<String> statement) {
    this.parser = parser;
    this.filterActions = new FilterActions(statement);
  }

  @ThreadSafe(safe = {FilterActions.class, SpelExpressionParser.class})
  @Override
  public Void decide(List<SyncData> dataList) {
    for (SyncData syncData : dataList) {
      StandardEvaluationContext context = syncData.getContext();
      filterActions.execute(parser, context);
    }
    return null;
  }
}