package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.MasterSourceType;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.mongo.DocTimestamp;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Registrant {

  private final List<InputSource> inputSources = new ArrayList<>();
  private final ConsumerRegistry consumerRegistry;
  private final String clientId;
  private final SchedulerBuilder schedulerBuilder;

  public Registrant(String clientId, ConsumerRegistry consumerRegistry,
      SchedulerBuilder schedulerBuilder) {
    this.consumerRegistry = consumerRegistry;
    this.clientId = clientId;
    this.schedulerBuilder = schedulerBuilder;
  }

  public Boolean register() {
    boolean res = true;
    for (InputSource inputSource : inputSources) {
      res = res && consumerRegistry.register(inputSource.getRemoteConnection(), inputSource);
    }
    return res;
  }

  public void addDatasource(MasterSource masterSource, SyncInitMeta syncInitMeta,
      MasterSourceType sourceType) {
    LocalInputSource inputSource;
    EventScheduler scheduler = schedulerBuilder.setSchedulerType(masterSource.getSchedulerType())
        .build();
    switch (sourceType) {
      case Mongo:
        Preconditions
            .checkState(syncInitMeta instanceof DocTimestamp, "syncInitMeta is " + syncInitMeta);
        inputSource = new MongoLocalInputSource(clientId, masterSource.getConnection(),
            masterSource.getSchemaSet(), (DocTimestamp) syncInitMeta, scheduler);
        break;
      case MySQL:
        Preconditions
            .checkState(syncInitMeta instanceof BinlogInfo, "syncInitMeta is " + syncInitMeta);
        inputSource = new MysqlLocalInputSource(clientId, masterSource.getConnection(),
            masterSource.getSchemaSet(), (BinlogInfo) syncInitMeta, scheduler);
        break;
      default:
        throw new IllegalStateException("Not implemented type");
    }
    inputSources.add(inputSource);
  }

}
