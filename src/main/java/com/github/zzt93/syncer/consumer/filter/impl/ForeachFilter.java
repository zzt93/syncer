package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.ForeachConfig;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class ForeachFilter implements ExprFilter, IfBodyAction {
  private final Logger logger = LoggerFactory.getLogger(ForeachFilter.class);

  private final SpelExpressionParser parser;
  private final FilterActions iterable;
  private final FilterActions statement;
  private final String var;

  public ForeachFilter(SpelExpressionParser parser, ForeachConfig foreach) {
    this.parser = parser;
    var = foreach.getVar();
    iterable = new FilterActions(foreach.getIn());
    statement = new FilterActions(foreach.getStatement());
  }


  @Override
  public Void decide(List<SyncData> dataList) {
    for (SyncData syncData : dataList) {
      execute(syncData);
    }
    return null;
  }

  @Override
  public Object execute(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    List<Object> res = iterable.execute(parser, context);
    if (!res.isEmpty()) {
      Object collectionOrArray = res.get(0);
      if (collectionOrArray instanceof Object[]) {
        for (Object o : ((Object[]) collectionOrArray)) {
          context.setVariable(var, o);
          statement.execute(parser, context);
        }
      } else if (collectionOrArray instanceof Iterable) {
        for (Object o : ((Iterable) collectionOrArray)) {
          context.setVariable(var, o);
          statement.execute(parser, context);
        }
      } else {
        logger
            .error("Impossible to iterate, have to use `Object[]` or `iterable` in `foreach.in`");
      }
    }
    return FilterRes.ACCEPT;
  }
}
