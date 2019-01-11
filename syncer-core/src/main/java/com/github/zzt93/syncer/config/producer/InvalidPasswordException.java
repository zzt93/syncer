package com.github.zzt93.syncer.config.producer;

/**
 * Created by zzt on 9/16/17.
 *
 * <h3></h3>
 */
public class InvalidPasswordException extends IllegalArgumentException {

  public InvalidPasswordException(String password) {
    super(password);
  }
}
