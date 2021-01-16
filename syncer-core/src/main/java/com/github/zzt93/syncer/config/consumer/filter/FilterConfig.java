package com.github.zzt93.syncer.config.consumer.filter;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.consumer.filter.impl.Drop;
import com.github.zzt93.syncer.consumer.filter.impl.ForeachFilter;
import com.github.zzt93.syncer.consumer.filter.impl.If;
import com.github.zzt93.syncer.consumer.filter.impl.JavaMethod;
import com.github.zzt93.syncer.consumer.filter.impl.Statement;
import com.github.zzt93.syncer.consumer.filter.impl.Switch;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
@ConsumerConfig("filter")
public class FilterConfig {

  private FilterType type;

  private Switcher switcher;
  private List<String> statement;
  private ForeachConfig foreach;
  @SerializedName(value = "if", alternate = {"If", "IF"})
  private IfConfig If;
  private Map drop;
  private String method;

  /*---the following field is not configured---*/
  private SyncerFilterMeta filterMeta;
  private String consumerId;

  public Switcher getSwitcher() {
    return switcher;
  }

  public void setSwitcher(Switcher switcher) {
    this.switcher = switcher;
  }

  public List<String> getStatement() {
    return statement;
  }

  public void setStatement(List<String> statement) {
    this.statement = statement;
  }

  public FilterType getType() {
    if (type == null) {
      if (switcher != null) {
        type = FilterType.SWITCH;
      }
      if (statement != null) {
        type = FilterType.STATEMENT;
      }
      if (foreach != null) {
        type = FilterType.FOREACH;
      }
      if (If != null) {
        type = FilterType.IF;
      }
      if (drop != null) {
        type = FilterType.DROP;
      }
      if (method != null) {
        type = FilterType.METHOD;
      }
    }
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
  }

  public IfConfig getIf() {
    return If;
  }

  public void setIf(IfConfig anIf) {
    this.If = anIf;
  }

  public Map getDrop() {
    return drop;
  }

  public void setDrop(Map drop) {
    this.drop = drop;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
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
      case METHOD:
        Preconditions.checkState(filterMeta != null, "Not set filterMeta for method");
        return JavaMethod.build(consumerId, filterMeta, getMethod());
      default:
        throw new InvalidConfigException("Unknown filter type");
    }
  }

  public FilterConfig addMeta(String consumerId, SyncerFilterMeta filterMeta) {
    this.filterMeta = filterMeta;
    this.consumerId = consumerId;
    return this;
  }


  public enum FilterType {
    SWITCH, STATEMENT, FOREACH, IF, DROP, METHOD;
  }
}
