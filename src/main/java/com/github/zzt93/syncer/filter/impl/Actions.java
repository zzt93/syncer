package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.common.expr.Expression;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.ParserContext;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class Actions implements Expression<Void> {

  private final Logger logger = LoggerFactory.getLogger(Actions.class);
  private final List<String> action;

  public Actions(List<String> action) {
    this.action = Collections.unmodifiableList(action);
  }

  @ThreadSafe(des = "immutable class")
  @Override
  public Void execute(ExpressionParser parser, EvaluationContext context) {
    for (String s : action) {
      try {
        parser.parseExpression(s, ParserContext.TEMPLATE_EXPRESSION).getValue(context);
      } catch (EvaluationException | ParseException e) {
        logger.error("Invalid expression {}, fail to parse", s, e);
      }
    }
    return null;
  }
}
