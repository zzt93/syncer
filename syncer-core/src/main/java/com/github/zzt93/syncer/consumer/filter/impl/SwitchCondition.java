package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class SwitchCondition implements Expression<String> {

  private final org.springframework.expression.Expression condition;

  public SwitchCondition(String condition, SpelExpressionParser parser) {
    this.condition = parser.parseExpression(condition);
  }

  @Override
  public String execute(EvaluationContext context) {
    Object value = condition.getValue(context);
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
