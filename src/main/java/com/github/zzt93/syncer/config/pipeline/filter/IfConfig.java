package com.github.zzt93.syncer.config.pipeline.filter;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.filter.impl.IfBodyAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class IfConfig {

  private String condition;
  private List<IfStatement> ifBody;
  private List<IfStatement> elseBody;

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public List<IfStatement> getIfBody() {
    return ifBody;
  }

  public void setIfBody(List<IfStatement> ifBody) {
    this.ifBody = ifBody;
  }

  public List<IfBodyAction> getIfAction(
      SpelExpressionParser parser) {
    return getBodyAction(ifBody, parser);
  }

  public List<IfBodyAction> getElseAction(
      SpelExpressionParser parser) {
    return getBodyAction(elseBody, parser);
  }

  private List<IfBodyAction> getBodyAction(List<IfStatement> body,
      SpelExpressionParser parser) {
    if (body == null) {
      return Collections.emptyList();
    }
    List<IfBodyAction> res = new ArrayList<>();
    for (IfStatement statement : body) {
      if (statement.getClone() != null) {
        res.add(new IfBodyAction(parser, statement.getClone()));
      } else if (statement.getDrop() != null) {
        res.add(new IfBodyAction(statement.getDrop()));
      } else if (statement.getStatement() != null) {
        res.add(new IfBodyAction(parser, statement.getStatement()));
      }
    }
    return res;
  }

  public List<IfStatement> getElseBody() {
    return elseBody;
  }

  public void setElseBody(List<IfStatement> elseBody) {
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

  private static class IfStatement {

    private CloneConfig clone;
    private List<String> statement;
    private Map drop;

    public CloneConfig getClone() {
      return clone;
    }

    public void setClone(CloneConfig clone) {
      this.clone = clone;
    }

    public List<String> getStatement() {
      return statement;
    }

    public void setStatement(List<String> statement) {
      this.statement = statement;
    }

    public Map getDrop() {
      return drop;
    }

    public void setDrop(Map drop) {
      this.drop = drop;
    }
  }

}
