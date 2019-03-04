package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.producer.register.ConsumerRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Registrant {

  private final List<ConsumerSource> consumerSources = new ArrayList<>();
  private final ConsumerRegistry consumerRegistry;

  public Registrant(ConsumerRegistry consumerRegistry) {
    this.consumerRegistry = consumerRegistry;
  }

  public Boolean register() {
    boolean res = true;
    for (ConsumerSource consumerSource : consumerSources) {
      res = res && consumerRegistry.register(consumerSource.getRemoteConnection(), consumerSource);
    }
    return res;
  }

  public void addDatasource(List<? extends ConsumerSource> consumerSource) {
    consumerSources.addAll(consumerSource);
  }

}
