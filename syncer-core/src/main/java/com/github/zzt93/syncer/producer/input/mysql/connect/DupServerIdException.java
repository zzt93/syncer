package com.github.zzt93.syncer.producer.input.mysql.connect;

/**
 * @author zzt
 */
public class DupServerIdException extends IllegalStateException {

  public DupServerIdException(Exception ex) {
    super(ex
    );
  }
}
