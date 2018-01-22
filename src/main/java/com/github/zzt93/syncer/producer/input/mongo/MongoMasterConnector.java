package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.config.pipeline.common.Connection;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;

/**
 * @author zzt
 */
public class MongoMasterConnector implements MasterConnector {

  public MongoMasterConnector(Connection connection, ConsumerRegistry consumerRegistry,
      int maxRetry) {

  }

  @Override
  public void run() {

  }
}
