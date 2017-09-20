package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import java.util.ArrayList;
import java.util.List;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Actions implements Expression<Void> {

  private final List<String> action;

  public Actions(List<String> action) {
    this.action = action;
  }

  @Override
  public Void execute(ExpressionParser parser, EvaluationContext context) {
    for (String s : action) {
      parser.parseExpression(s, ParserContext.TEMPLATE_EXPRESSION).getValue(context);
    }
    return null;
  }
}
