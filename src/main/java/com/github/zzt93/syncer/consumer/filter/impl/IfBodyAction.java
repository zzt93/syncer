package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import org.springframework.expression.ExpressionParser;

/**
 * @author zzt
 */
public interface IfBodyAction {

  @ThreadSafe(safe = {ExpressionParser.class})
  Object execute(SyncData data);
}
