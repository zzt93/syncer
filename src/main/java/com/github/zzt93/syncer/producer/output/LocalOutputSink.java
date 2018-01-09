package com.github.zzt93.syncer.producer.output;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.consumer.InputSource;
import java.util.List;

/**
 * @author zzt
 */
public class LocalOutputSink implements OutputSink {

  private InputSource inputSource;

  public LocalOutputSink(InputSource inputSource) {
    this.inputSource = inputSource;
  }

  @Override
  public boolean output(SyncData data) {
    return inputSource.input(data);
  }

  @Override
  public boolean output(SyncData[] data) {
    return inputSource.input(data);
  }

  @Override
  public List<String> accept() {
    return null;
  }

}
