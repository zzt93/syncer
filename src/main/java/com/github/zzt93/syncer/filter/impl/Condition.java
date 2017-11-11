package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Condition implements Expression<Boolean> {

  private final String condition;

  public Condition(String condition) {
    if (condition == null) {
      throw new InvalidConfigException("switcher.switch is a must, can't be null");
    }
    this.condition = condition;
  }

  @Override
  public Boolean execute(ExpressionParser parser, EvaluationContext context) {
    Boolean value = parser.parseExpression(condition)
        .getValue(context, Boolean.class);
    if (value == null) {
      return false;
    }
    return value;
  }
}
