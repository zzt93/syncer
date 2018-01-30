package com.github.zzt93.syncer.producer.register;

import static com.google.common.base.Preconditions.checkState;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.consumer.input.MongoInputSource;
import com.github.zzt93.syncer.consumer.input.MysqlInputSource;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.output.LocalOutputSink;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.google.common.collect.Sets;
import java.util.IdentityHashMap;
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
public class LocalConsumerRegistry implements ConsumerRegistry {

  private Logger logger = LoggerFactory.getLogger(LocalConsumerRegistry.class);

  private ConcurrentHashMap<Connection, BinlogInfo> olderBinlog = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Connection, DocTimestamp> smallerId = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<Connection> voted = new ConcurrentSkipListSet<>();
  private ConcurrentHashMap<Connection, Set<InputSource>> inputSources = new ConcurrentHashMap<>();

  @Override
  public boolean register(Connection connection, InputSource source) {
    // TODO 18/1/9 unused connection
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
    }
    final boolean[] add = new boolean[1];
    inputSources.compute(connection, (k, v) -> {
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
    return olderBinlog.get(connection);
  }

  @Override
  public DocTimestamp votedMongoId(Connection connection) {
    checkState(smallerId.containsKey(connection), "no input source registered");
    voted.add(connection);
    return smallerId.get(connection);
  }

  @Override
  public IdentityHashMap<Set<Schema>, OutputSink> outputSink(Connection connection) {
    checkState(inputSources.containsKey(connection), "no input source registered");
    IdentityHashMap<Set<Schema>, OutputSink> res = new IdentityHashMap<>();
    // TODO 18/1/15 may reuse but not new
    for (InputSource inputSource : inputSources.get(connection)) {
      res.put(inputSource.getSchemas(), new LocalOutputSink(inputSource));
    }
    return res;
  }

}
