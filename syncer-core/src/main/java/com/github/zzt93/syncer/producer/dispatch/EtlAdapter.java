package com.github.zzt93.syncer.producer.dispatch;

public interface EtlAdapter {

  void markColdStart(String repo, String entity);

  void markColdStartDoneAndFlush();

  boolean needColdStart();
}
