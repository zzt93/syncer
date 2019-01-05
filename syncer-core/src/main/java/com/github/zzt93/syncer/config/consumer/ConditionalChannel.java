package com.github.zzt93.syncer.config.consumer;

/**
 * @author zzt
 */
public interface ConditionalChannel {

  default String conditionExpr() {
    return null;
  }

}
