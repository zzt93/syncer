package com.github.zzt93.syncer.config.pipeline;

/**
 * @author zzt
 */
public interface ConditionalChannel {

  default String conditionExpr() {
    return null;
  }

}
