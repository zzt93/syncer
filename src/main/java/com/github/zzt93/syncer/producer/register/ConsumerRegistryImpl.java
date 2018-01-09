package com.github.zzt93.syncer.producer.register;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.output.LocalOutputSink;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author zzt
 */
@Component
public class ConsumerRegistryImpl implements ConsumerRegistry {

  private Logger logger = LoggerFactory.getLogger(ConsumerRegistryImpl.class);

  private ConcurrentHashMap<Connection, BinlogInfo> olderBinlog = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Connection, Boolean> voted = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Connection, Set<InputSource>> inputSources = new ConcurrentHashMap<>();

  @Override
  public boolean register(Connection connection, InputSource source) {
    // TODO 18/1/9 unused connection
    if (voted.get(connection)) {
      logger.warn("Output sink is already started, fail to register");
      return false;
    }
    olderBinlog.compute(connection, (k, v) -> v == null ? source.getBinlogInfo() :
        v.compareTo(source.getBinlogInfo()) <= 0 ? v : source.getBinlogInfo());
    final boolean[] add = new boolean[1];
    inputSources.compute(connection, (k, v) -> {
      if (v == null) {
        return Sets.newHashSet(source);
      } else {
        add[0] = v.add(source);
        if (!add[0]) {
          logger.warn("Duplicate input source {}", source.clientId());
        }
        return v;
      }
    });
    return add[0];
  }

  @Override
  public BinlogInfo votedBinlogInfo(Connection connection) {
    Preconditions.checkState(olderBinlog.containsKey(connection), "no input source registered");
    voted.put(connection, true);
    return olderBinlog.get(connection);
  }

  @Override
  public Set<OutputSink> outputSink(MysqlConnection connection) {
    Preconditions.checkState(inputSources.containsKey(connection), "no input source registered");
    Preconditions.checkState(voted.containsKey(connection), "should invoke votedBinlogInfo first");
    Set<OutputSink> res = new HashSet<>();
    for (InputSource inputSource : inputSources.get(connection)) {
      res.add(new LocalOutputSink(inputSource));
    }
    return res;
  }

}
