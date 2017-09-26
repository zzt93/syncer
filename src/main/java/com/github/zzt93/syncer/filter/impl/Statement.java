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
  private final Actions actions;

  public Statement(SpelExpressionParser parser, List<String> statement) {
    this.parser = parser;
    this.actions = new Actions(statement);
  }

  @ThreadSafe(safe = {Actions.class, SpelExpressionParser.class})
  @Override
  public FilterRes decide(SyncData e) {
    StandardEvaluationContext context = new StandardEvaluationContext(e);
    actions.execute(parser, context);
    return null;
  }
}
