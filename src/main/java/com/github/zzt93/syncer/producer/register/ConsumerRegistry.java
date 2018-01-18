package com.github.zzt93.syncer.producer.register;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * @author zzt
 */
public interface ConsumerRegistry {

  boolean register(Connection connection,
      InputSource source);

  BinlogInfo votedBinlogInfo(Connection connection);

  IdentityHashMap<Set<Schema>, OutputSink> outputSink(Connection connection);
}
