package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.filter.ExprFilter;
import java.util.HashMap;
import java.util.Map;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Switch implements ExprFilter {

  private final SpelExpressionParser parser;
  private final Condition condition;
  private final Map<String, Actions> action = new HashMap<>();

  public Switch(SpelExpressionParser parser, FilterConfig filter) {
    condition = new Condition(filter.getCondition());
    this.parser = parser;
    filter.getAction().forEach((k, v) -> action.put(k, new Actions(v)));
  }

  /**
   * <a href="https://stackoverflow.com/questions/16775203/is-spelexpression-in-spring-el-thread-safe">
   *   SpelExpressionParser is thread safe</a>
   */
  @Override
  public FilterRes decide(SyncData data) {
    StandardEvaluationContext context = new StandardEvaluationContext(data);
    String conditionRes = condition.execute(parser, context);
    action.get(conditionRes).execute(parser, context);
    return FilterRes.ACCEPT;
  }
}
