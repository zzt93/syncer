package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.data.CommonTypeLocator;
import com.github.zzt93.syncer.common.data.SyncData;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class SyncDeserializer {

  public static void afterRecover(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    context.setTypeLocator(new CommonTypeLocator());
    context.setRootObject(data);
  }
}
