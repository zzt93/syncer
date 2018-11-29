package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.ForeachConfig;
import com.github.zzt93.syncer.consumer.filter.CompositeStatement;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class ForeachFilter implements CompositeStatement {
  private final Logger logger = LoggerFactory.getLogger(ForeachFilter.class);

  private final String var;
  private final Statement iterable;
  private final List<ExprFilter> statements;

  public ForeachFilter(SpelExpressionParser parser, ForeachConfig foreach) {
    var = foreach.getVar();
    iterable = new Statement(parser, foreach.getIn());
    statements = foreach.getStatement()
        .stream()
        .map(c -> c.toFilter(parser))
        .collect(Collectors.toList());
  }

  @Override
  public void recurWithSingleElement(LinkedList<SyncData> tmp) {
    assert tmp.size() == 1;
    SyncData data = tmp.getFirst();
    StandardEvaluationContext context = data.getContext();
    Object collectionOrArray = iterable.execute(data).get(0);

    if (collectionOrArray instanceof Object[]) {
      for (Object o : ((Object[]) collectionOrArray)) {
        context.setVariable(var, o);
        for (ExprFilter filter : statements) {
          filter.filter(tmp);
        }
      }
    } else if (collectionOrArray instanceof Iterable) {
      for (Object o : ((Iterable) collectionOrArray)) {
        context.setVariable(var, o);
        for (ExprFilter filter : statements) {
          filter.filter(tmp);
        }
      }
    } else {
      logger
          .error("Impossible to iterate, have to use `Object[]` or `iterable` in `foreach.in`");
    }
  }

}
