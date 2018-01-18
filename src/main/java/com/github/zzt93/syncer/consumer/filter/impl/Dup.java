package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.DupConfig;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class Dup implements ExprFilter, IfBodyAction {

  private final List<FilterActions> newObjAction;
  private final SpelExpressionParser parser;
  private final List<String> copyValue;

  public Dup(SpelExpressionParser parser, DupConfig dupConfig) {
    this.parser = parser;
    copyValue = dupConfig.getCopyValue();
    newObjAction = dupConfig.getNew().stream().map(FilterActions::new).collect(Collectors.toList());
  }

  @Override
  public Object execute(SyncData src) {
    LinkedList<SyncData> res = new LinkedList<>();
    for (FilterActions filterActions : newObjAction) {
      SyncData dup = new SyncData(src);
      for (String s : copyValue) {
        Object value = parser.parseExpression(s).getValue(src.getContext());
        parser.parseExpression(s).setValue(dup.getContext(), value);
      }
      filterActions.execute(parser, dup.getContext());
      res.add(dup);
    }
    return res;
  }

  @Override
  public Void decide(List<SyncData> e) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
