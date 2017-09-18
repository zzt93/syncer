package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Condition implements Expression<String> {

  private final String condition;

  public Condition(String condition) {
    this.condition = condition;
  }

  @Override
  public String execute(ExpressionParser parser, EvaluationContext context) {
    Object value = parser.parseExpression(condition, ParserContext.TEMPLATE_EXPRESSION).getValue(context);
    if (value == null) {
      return "";
    }
    return value.toString();
  }
}
