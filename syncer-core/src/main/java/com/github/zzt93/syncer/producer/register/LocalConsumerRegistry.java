package com.github.zzt93.syncer.producer.register;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.consumer.input.MongoInputSource;
import com.github.zzt93.syncer.consumer.input.MysqlInputSource;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.output.LocalProducerSink;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author zzt
 */
public class LocalConsumerRegistry implements ConsumerRegistry {

  private Logger logger = LoggerFactory.getLogger(LocalConsumerRegistry.class);

  private ConcurrentHashMap<Connection, BinlogInfo> olderBinlog = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Connection, DocTimestamp> smallerId = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<Connection> voted = new ConcurrentSkipListSet<>();
  private ConcurrentHashMap<Connection, Set<ConsumerSource>> consumerSources = new ConcurrentHashMap<>();

  @Override
  public boolean register(Connection connection, ConsumerSource source) {
    if (voted.contains(connection)) {
      logger.warn("Output sink is already started, fail to register");
      return false;
    }
    if (source instanceof MysqlInputSource) {
      BinlogInfo syncInitMeta = ((MysqlInputSource) source).getSyncInitMeta();
      olderBinlog.compute(connection, (k, v) -> v == null ? syncInitMeta :
          v.compareTo(syncInitMeta) <= 0 ? v : syncInitMeta);
    } else if (source instanceof MongoInputSource) {
      DocTimestamp syncInitMeta = ((MongoInputSource) source).getSyncInitMeta();
      smallerId.compute(connection, (k, v) -> v == null ? syncInitMeta :
          v.compareTo(syncInitMeta) <= 0 ? v : syncInitMeta);
    } else {
      checkState(false);
    }
    final boolean[] add = new boolean[1];
    consumerSources.compute(connection, (k, v) -> {
      if (v == null) {
        add[0] = true;
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
    checkState(olderBinlog.containsKey(connection), "no input source registered");
    voted.add(connection);
    BinlogInfo binlogInfo = olderBinlog.get(connection);
    logger.info("Voted {} for {}", binlogInfo, connection);
    return binlogInfo;
  }

  @Override
  public DocTimestamp votedMongoId(Connection connection) {
    checkState(smallerId.containsKey(connection), "no input source registered");
    voted.add(connection);
    DocTimestamp docTimestamp = smallerId.get(connection);
    logger.info("Voted {} for {}", docTimestamp, connection);
    return docTimestamp;
  }

  @Override
  public HashMap<Consumer, ProducerSink> outputSink(Connection connection) {
    HashMap<Consumer, ProducerSink> res = new HashMap<>();
    if (!consumerSources.containsKey(connection)) return res;
    for (ConsumerSource consumerSource : consumerSources.get(connection)) {
      res.put(new Consumer(consumerSource), new LocalProducerSink(consumerSource));
    }
    return res;
  }

  @Override
  public Set<Connection> wantedSource() {
    return new HashSet<>(consumerSources.keySet());
  }

}
