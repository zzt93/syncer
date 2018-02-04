package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class SwitchCondition implements Expression<String> {

  private final String condition;

  public SwitchCondition(String condition) {
    this.condition = condition;
  }

  @Override
  public String execute(ExpressionParser parser, EvaluationContext context) {
    Object value = parser.parseExpression(condition).getValue(context);
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
