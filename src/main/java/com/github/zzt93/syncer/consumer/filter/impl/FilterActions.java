package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.expr.Expression;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class FilterActions implements Expression<List<Object>> {

  private final Logger logger = LoggerFactory.getLogger(FilterActions.class);
  private final List<org.springframework.expression.Expression> action;


  public FilterActions(SpelExpressionParser parser, List<String> action) {
    this.action = Collections.unmodifiableList(
        action.stream().map(parser::parseExpression).collect(Collectors.toList()));
  }

  public FilterActions(SpelExpressionParser parser, String expr) {
    this.action = Collections.singletonList(parser.parseExpression(expr));
  }

  @ThreadSafe(des = "immutable class")
  @Override
  public List<Object> execute(EvaluationContext context) {
    ArrayList<Object> res = new ArrayList<>();
    for (org.springframework.expression.Expression s : action) {
      try {
        Object value = s.getValue(context);
        if (value != null) {
          res.add(value);
        }
      } catch (EvaluationException | ParseException e) {
        logger.error("Invalid expression {}, fail to parse", s, e);
      }
    }
    return res;
  }
}
