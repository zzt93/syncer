package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Registrant {

  private final List<InputSource> inputSources = new ArrayList<>();
  private final ConsumerRegistry consumerRegistry;
  private final String clientId;

  Registrant(String clientId, ConsumerRegistry consumerRegistry) {
    this.consumerRegistry = consumerRegistry;
    this.clientId = clientId;
  }

  Boolean register() {
    boolean res= true;
    for (InputSource inputSource : inputSources) {
      res = res && consumerRegistry.register(inputSource.getRemoteConnection(), inputSource);
    }
    return res;
  }

  void addDatasource(MysqlMaster mysqlMaster, BinlogInfo binlogInfo) {
    LocalInputSource inputSource = new LocalInputSource(consumerRegistry,
        mysqlMaster.getSchemas(),
        mysqlMaster.getConnection(), binlogInfo, clientId);
    inputSources.add(inputSource);
  }
}
