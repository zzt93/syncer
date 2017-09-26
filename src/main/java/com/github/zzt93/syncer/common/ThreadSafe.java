package com.github.zzt93.syncer.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author zzt
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface ThreadSafe {

  String des() default "";

  Class[] safe() default {};

  Class[] toCheck() default {};
}
