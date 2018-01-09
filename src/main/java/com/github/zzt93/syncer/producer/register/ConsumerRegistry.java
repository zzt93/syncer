package com.github.zzt93.syncer.producer.register;

import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.output.OutputSink;
import java.util.Set;

/**
 * @author zzt
 */
public interface ConsumerRegistry {

  boolean register(InputSource source);

  BinlogInfo votedBinlogInfo();

  Set<OutputSink> output();
}
