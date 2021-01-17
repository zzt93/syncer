package com.github.zzt93.syncer.config.consumer.filter;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.consumer.filter.impl.JavaMethod;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.common.base.Preconditions;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
@ConsumerConfig("filter")
public class FilterConfig {

  private FilterType type;

  private String method;

  /*---the following field is not configured---*/
  private SyncerFilterMeta filterMeta;
  private String consumerId;


  public FilterType getType() {
    if (type == null) {
      if (method != null) {
        type = FilterType.METHOD;
      }
    }
    return type;
  }

  public void setType(FilterType type) {
    this.type = type;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public SyncFilter toFilter(SpelExpressionParser parser) {
    if (getType() == FilterType.METHOD) {
      Preconditions.checkState(filterMeta != null, "Not set filterMeta for method");
      return JavaMethod.build(consumerId, filterMeta, getMethod());
    }
    throw new InvalidConfigException("Unknown filter type");
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
