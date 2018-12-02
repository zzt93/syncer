package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.consumer.filter.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class Statement implements SimpleStatement {

  private final Logger logger = LoggerFactory.getLogger(Statement.class);
  private final List<Expression> action;

  public Statement(SpelExpressionParser parser, List<String> action) {
    this.action = Collections.unmodifiableList(
        action.stream().map(parser::parseExpression).collect(Collectors.toList()));
  }

  public Statement(SpelExpressionParser parser, String expr) {
    this.action = Collections.singletonList(parser.parseExpression(expr));
  }

  /**
   * @return expression execution result, may has null value
   */
  @ThreadSafe(des = "immutable class")
  public List<Object> execute(SyncData syncData) {
    ArrayList<Object> res = new ArrayList<>();
    for (Expression s : action) {
      try {
        res.add(s.getValue(syncData.getContext()));
      } catch (EvaluationException | ParseException e) {
        logger.error("Invalid expression {} for {}, fail to parse", s.getExpressionString(), syncData, e);
      }
    }
    return res;
  }

  @ThreadSafe(safe = {SpelExpressionParser.class})
  @Override
  public void filter(List<SyncData> dataList) {
    for (SyncData syncData : dataList) {
      execute(syncData);
    }
  }

}
