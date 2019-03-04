package com.github.zzt93.syncer.consumer.output;

/**
 * @author zzt
 */
public interface Retryable {

  void inc();

  int retryCount();

}
