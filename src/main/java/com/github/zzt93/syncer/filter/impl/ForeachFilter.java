package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.Foreach;
import com.github.zzt93.syncer.filter.ExprFilter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class ForeachFilter implements ExprFilter {
  private final Logger logger = LoggerFactory.getLogger(ForeachFilter.class);

  private final SpelExpressionParser parser;
  private final Actions expr;
  private final Actions statement;
  private final String var;

  public ForeachFilter(SpelExpressionParser parser, Foreach foreach) {
    this.parser = parser;
    var = foreach.getVar();
    expr = new Actions(foreach.getIn());
    statement = new Actions(foreach.getStatement());
  }


  @Override
  public FilterRes decide(SyncData e) {
    StandardEvaluationContext context = e.getContext();
    List<Object> res = expr.execute(parser, context);
    if (!res.isEmpty()) {
      Object collectionOrArray = res.get(0);
      if (collectionOrArray instanceof Object[]) {
        for (Object o : ((Object[]) collectionOrArray)) {
          context.setVariable(var, o);
          statement.execute(parser, context);
        }
      } else if (collectionOrArray instanceof Iterable){
        for (Object o : ((Iterable) collectionOrArray)) {
          context.setVariable(var, o);
          statement.execute(parser, context);
        }
      } else {
        logger.warn("Impossible to iterate, have to use `Object[]` or `iterable` in `foreach.in`");
      }
    }
    return FilterRes.ACCEPT;
  }
}
