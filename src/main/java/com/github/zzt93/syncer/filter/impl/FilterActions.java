package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.common.expr.Expression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class FilterActions implements Expression<List<Object>> {

  private final Logger logger = LoggerFactory.getLogger(FilterActions.class);
  private final List<String> action;


  public FilterActions(List<String> action) {
    this.action = Collections.unmodifiableList(action);
  }

  public FilterActions(String expr) {
    this.action = Collections.singletonList(expr);
  }

  @ThreadSafe(des = "immutable class")
  @Override
  public List<Object> execute(ExpressionParser parser, EvaluationContext context) {
    ArrayList<Object> res = new ArrayList<>();
    for (String s : action) {
      try {
        Object value = parser.parseExpression(s)
            .getValue(context);
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
