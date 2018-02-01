package com.github.zzt93.syncer.common.data;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class SyncDataExclusion implements ExclusionStrategy {

  public boolean shouldSkipClass(Class<?> arg0) {
    return false;
  }

  public boolean shouldSkipField(FieldAttributes f) {
    return (f.getDeclaringClass() == StandardEvaluationContext.class && !f.getName().equals("variables"));
  }

}
