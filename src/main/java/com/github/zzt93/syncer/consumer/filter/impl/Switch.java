package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.filter.Switcher;
import com.github.zzt93.syncer.consumer.filter.ExprFilter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class Switch implements ExprFilter, IfBodyAction {

  private final Logger logger = LoggerFactory.getLogger(Switch.class);
  private final SpelExpressionParser parser;
  private final SwitchCondition switchCondition;
  private final Map<String, FilterActions> actionsMap;

  public Switch(SpelExpressionParser parser, Switcher filter) {
    switchCondition = new SwitchCondition(filter.getSwitch(), parser);
    this.parser = parser;
    Map<String, FilterActions> tmp = new HashMap<>();
    filter.getCase().forEach((k, v) -> tmp.put(k, new FilterActions(parser, v)));
    actionsMap = Collections.unmodifiableMap(tmp);
  }

  /**
   * <a href="https://stackoverflow.com/questions/16775203/is-spelexpression-in-spring-el-thread-safe">
   * SpelExpressionParser is thread safe</a>
   */
  @ThreadSafe(safe = {Condition.class, SpelExpressionParser.class, FilterActions.class})
  @Override
  public Void decide(List<SyncData> data) {
    // TODO 18/2/5 add drop
    for (SyncData syncData : data) {
      execute(syncData);
    }
    return null;
  }

  @Override
  public Object execute(SyncData syncData) {
    StandardEvaluationContext context = syncData.getContext();
    String conditionRes = switchCondition.execute(context);
    if (conditionRes == null) {
      logger.warn("switch on `null`, skip {}", syncData);
      return FilterRes.DENY;
    }
    FilterActions filterActions = actionsMap.get(conditionRes);
    if (filterActions != null) {
      filterActions.execute(context);
    } else if (actionsMap.containsKey(Switcher.DEFAULT)) {
      actionsMap.get(Switcher.DEFAULT).execute(context);
    }
    return FilterRes.ACCEPT;
  }
}
