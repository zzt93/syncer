package com.github.zzt93.syncer.config.consumer.filter;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.data.SyncFilter;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zzt
 */
public class IfConfig {

  private String condition;
  private List<FilterConfig> ifBody;
  private List<FilterConfig> elseBody;

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public List<FilterConfig> getIfBody() {
    return ifBody;
  }

  public void setIfBody(List<FilterConfig> ifBody) {
    this.ifBody = ifBody;
  }

  public List<SyncFilter> getIfAction(
      SpelExpressionParser parser) {
    return getBodyAction(ifBody, parser);
  }

  public List<SyncFilter> getElseAction(
      SpelExpressionParser parser) {
    return getBodyAction(elseBody, parser);
  }

  private List<SyncFilter> getBodyAction(List<FilterConfig> body,
                                         SpelExpressionParser parser) {
    if (body == null) {
      return Collections.emptyList();
    }
    List<SyncFilter> res = new ArrayList<>();
    for (FilterConfig statement : body) {
      res.add(statement.toFilter(parser));
    }
    return res;
  }

  public List<FilterConfig> getElseBody() {
    return elseBody;
  }

  public void setElseBody(List<FilterConfig> elseBody) {
    this.elseBody = elseBody;
  }

  public void checkConfig() {
    if (getCondition() == null) {
      throw new InvalidConfigException("Lack `condition` in `if`, which is a must");
    }
    if (getIfBody() == null) {
      throw new InvalidConfigException("Lack `if-body` in `if`, which is a must");
    }
  }

}
