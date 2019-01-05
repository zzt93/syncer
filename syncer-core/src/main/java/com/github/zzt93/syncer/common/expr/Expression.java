package com.github.zzt93.syncer.common.expr;

import org.springframework.expression.EvaluationContext;

/**
 * @author zzt
 */
public interface Expression<O> {

  O execute(EvaluationContext context);
  
}
