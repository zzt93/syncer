package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

/**
 * @author zzt
 */
public class Registrant {

  private final List<InputSource> inputSources = new ArrayList<>();
  private final ConsumerRegistry consumerRegistry;
  private final String clientId;
  private final BlockingDeque<SyncData> filterInput;

  Registrant(String clientId, ConsumerRegistry consumerRegistry,
      BlockingDeque<SyncData> filterInput) {
    this.consumerRegistry = consumerRegistry;
    this.clientId = clientId;
    this.filterInput = filterInput;
  }

  Boolean register() {
    boolean res= true;
    for (InputSource inputSource : inputSources) {
      res = res && consumerRegistry.register(inputSource.getRemoteConnection(), inputSource);
    }
    return res;
  }

  void addMySQLDatasource(MysqlMaster mysqlMaster, BinlogInfo syncInitMeta) {
    LocalInputSource inputSource = new MySQLLocalInputSource(
        clientId, mysqlMaster.getConnection(), mysqlMaster.getSchemaSet(),
        syncInitMeta, filterInput);
    inputSources.add(inputSource);
  }

}
