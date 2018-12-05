package com.github.zzt93.syncer.config.consumer.filter;

import com.github.zzt93.syncer.config.consumer.common.InvalidConfigException;
import com.github.zzt93.syncer.data.SyncFilter;
import com.github.zzt93.syncer.consumer.filter.impl.*;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class FilterConfig {

  private FilterType type;

  private Switcher switcher;
  private List<String> statement;
  private ForeachConfig foreach;
  private IfConfig If;
  private Map drop;
  private CreateConfig create;
  private String method;

  public CreateConfig getCreate() {
    return create;
  }

  public void setCreate(CreateConfig create) {
    this.create = create;
    type = FilterType.CREATE;
  }

  public Switcher getSwitcher() {
    return switcher;
  }

  public void setSwitcher(Switcher switcher) {
    this.switcher = switcher;
    type = FilterType.SWITCH;
  }

  public List<String> getStatement() {
    return statement;
  }

  public void setStatement(List<String> statement) {
    this.statement = statement;
    type = FilterType.STATEMENT;
  }

  public FilterType getType() {
    return type;
  }

  public void setType(FilterType type) {
    this.type = type;
  }

  public ForeachConfig getForeach() {
    return foreach;
  }

  public void setForeach(ForeachConfig foreach) {
    this.foreach = foreach;
    type = FilterType.FOREACH;
  }

  public IfConfig getIf() {
    return If;
  }

  public void setIf(IfConfig anIf) {
    this.If = anIf;
    type = FilterType.IF;
  }

  public Map getDrop() {
    return drop;
  }

  public void setDrop(Map drop) {
    this.drop = drop;
    type = FilterType.DROP;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
    type = FilterType.METHOD;
  }

  public SyncFilter toFilter(SpelExpressionParser parser) {
    switch (getType()) {
      case SWITCH:
        return new Switch(parser, getSwitcher());
      case STATEMENT:
        return new Statement(parser, getStatement());
      case FOREACH:
        return new ForeachFilter(parser, getForeach());
      case IF:
        return new If(parser, getIf());
      case DROP:
        return new Drop();
      case CREATE:
        try {
          return getCreate().toAction(parser);
        } catch (NoSuchFieldException e) {
          throw new InvalidConfigException("Unknown field of `SyncData` to copy", e);
        }
      case METHOD:
        return JavaMethod.build(getMethod());
      default:
        throw new InvalidConfigException("Unknown filter type");
    }
  }


  public enum FilterType {
    SWITCH, STATEMENT, FOREACH, IF, DROP, CREATE, METHOD;
  }
}
