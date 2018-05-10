package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Registrant {

  private final List<InputSource> inputSources = new ArrayList<>();
  private final ConsumerRegistry consumerRegistry;

  public Registrant(ConsumerRegistry consumerRegistry) {
    this.consumerRegistry = consumerRegistry;
  }

  public Boolean register() {
    boolean res = true;
    for (InputSource inputSource : inputSources) {
      res = res && consumerRegistry.register(inputSource.getRemoteConnection(), inputSource);
    }
    return res;
  }

  public void addDatasource(InputSource inputSource) {
    inputSources.add(inputSource);
  }

}
