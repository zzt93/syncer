package com.github.zzt93.syncer.common.thread;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author zzt
 */
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.SOURCE)
public @interface ThreadSafe {

  String des() default "";

  String[] sharedBy() default {};

  Class[] safe() default {};

  Class[] toCheck() default {};
}
