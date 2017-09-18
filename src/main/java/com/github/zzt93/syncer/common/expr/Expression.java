package com.github.zzt93.syncer.common.expr;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;

/**
 * @author zzt
 */
public interface Expression<O> {

  O execute(ExpressionParser parser, EvaluationContext context);
  
}
