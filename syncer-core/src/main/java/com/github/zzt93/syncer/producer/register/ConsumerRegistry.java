package com.github.zzt93.syncer.producer.register;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.consumer.ConsumerSource;
import com.github.zzt93.syncer.producer.ProducerStarter;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.input.mysql.meta.Consumer;
import com.github.zzt93.syncer.producer.output.ProducerSink;

import java.util.HashMap;
import java.util.Set;

/**
 * @author zzt
 */
public interface ConsumerRegistry {

  boolean register(Connection connection, ConsumerSource source);

  BinlogInfo votedBinlogInfo(Connection connection);

  DocTimestamp votedMongoId(Connection connection);

  HashMap<Consumer, ProducerSink> outputSink(Connection connection);

  /**
   * should return a copy of wanted source
   * @see ProducerStarter#start()
   */
  Set<Connection> wantedSource();
}
