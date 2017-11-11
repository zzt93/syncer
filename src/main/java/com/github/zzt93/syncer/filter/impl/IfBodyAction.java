package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.Filter.FilterRes;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.filter.CloneConfig;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class IfBodyAction {

  private Statement statement;
  private Clone clone;
  private Drop drop;

  public IfBodyAction(SpelExpressionParser parser, CloneConfig clone) {
    try {
      this.clone = new Clone(parser, clone);
    } catch (NoSuchFieldException e) {
      throw new InvalidConfigException("Unknown field of `SyncData`", e);
    }
  }

  public IfBodyAction(Map drop) {
    this.drop = new Drop();
  }

  public IfBodyAction(SpelExpressionParser parser,
      List<String> statement) {
    this.statement = new Statement(parser, statement);
  }

  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement statement) {
    this.statement = statement;
  }

  public Clone getClone() {
    return clone;
  }

  public void setClone(Clone clone) {
    this.clone = clone;
  }

  public Drop getDrop() {
    return drop;
  }

  public void setDrop(Drop drop) {
    this.drop = drop;
  }

  @ThreadSafe(safe = {ExpressionParser.class})
  public Object execute(SyncData data) {
    if (statement != null) {
      statement.decide(Collections.singletonList(data));
      return FilterRes.ACCEPT;
    } else if (drop != null) {
      return FilterRes.DENY;
    } else if (clone != null) {
      LinkedList<SyncData> list = new LinkedList<>();
      list.add(data);
      clone.decide(list);
      return list.getLast();
    }
    throw new IllegalStateException("Impossible to reach here");
  }
}
