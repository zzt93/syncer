package com.github.zzt93.syncer.producer.register;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.output.LocalOutputSink;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author zzt
 */
@Component
public class ConsumerRegistryImpl implements ConsumerRegistry {

  private Logger logger = LoggerFactory.getLogger(ConsumerRegistryImpl.class);

  private ConcurrentSkipListSet<InputSource> inputSources = new ConcurrentSkipListSet<>();
  private ConcurrentHashMap<Connection, BinlogInfo> binlogInfos = new ConcurrentHashMap<>();

  @Override
  public boolean register(InputSource source) {
    if (older.compareAndSet()) {
      logger.warn("Output sink is already started, fail to register");
      return false;
    }
    boolean add = inputSources.add(source);
    if (!add) {
      logger.warn("Duplicate input source {}", source.clientId());
    }
    return add;
  }

  @Override
  public BinlogInfo votedBinlogInfo() {
    older.
  }

  @Override
  public Set<OutputSink> output() {
    Set<OutputSink> res = new HashSet<>();
    for (InputSource inputSource : inputSources) {
      res.add(new LocalOutputSink(inputSource));
    }
    return res;
  }

}
