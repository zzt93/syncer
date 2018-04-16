package com.github.zzt93.syncer.common.data;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class EvaluationFactory {
  private static final MapAccessor accessor = new MapAccessor();

  public static StandardEvaluationContext context() {
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setTypeLocator(new CommonTypeLocator());
    context.addPropertyAccessor(accessor);
    return context;
  }

}
