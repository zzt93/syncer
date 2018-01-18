package com.github.zzt93.syncer.consumer.ack;

/**
 * @author zzt
 */
public interface Retryable {

  void inc();

  int count();

}
