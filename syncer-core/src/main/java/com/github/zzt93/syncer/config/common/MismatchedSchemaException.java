package com.github.zzt93.syncer.config.common;

/**
 * @author zzt
 */
public class MismatchedSchemaException extends IllegalStateException {

  public MismatchedSchemaException(String s, ArrayIndexOutOfBoundsException e) {
    super(s, e);
  }
}
