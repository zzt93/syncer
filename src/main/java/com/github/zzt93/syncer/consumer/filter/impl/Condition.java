package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Condition implements Expression<Boolean> {

  private final org.springframework.expression.Expression condition;

  public Condition(SpelExpressionParser parser, String condition) {
    if (condition == null) {
      throw new InvalidConfigException("switcher.switch is a must, can't be null");
    }
    this.condition = parser.parseExpression(condition);
  }

  @Override
  public Boolean execute(EvaluationContext context) {
    Boolean value = condition.getValue(context, Boolean.class);
    if (value == null) {
      return false;
    }
    return value;
  }
}
