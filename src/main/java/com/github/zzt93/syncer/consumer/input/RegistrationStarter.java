package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.pipeline.input.SyncMeta;
import com.github.zzt93.syncer.config.syncer.SyncerAck;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.producer.input.mysql.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class RegistrationStarter implements Starter<PipelineInput, Set<MasterSource>> {

  private final Logger logger = LoggerFactory.getLogger(RegistrationStarter.class);
  private final SyncerAck ackConfig;
  private Registrant registrant;
  private Ack ack;

  public RegistrationStarter(PipelineInput pipelineInput,
      SyncerAck ackConfig, SyncerInput input,
      ConsumerRegistry consumerRegistry, String consumerId,
      BlockingDeque<SyncData> filterInput)  {
    registrant = new Registrant(consumerId, consumerRegistry, filterInput);
    HashMap<String, SyncInitMeta> id2SyncInitMeta = new HashMap<>();
    Set<MasterSource> masterSet = pipelineInput.getMasterSet();
    this.ack = Ack.build(consumerId, input.getInputMeta(), masterSet, id2SyncInitMeta);
    for (MasterSource masterSource : masterSet) {
      String identifier = masterSource.getConnection().connectionIdentifier();
      SyncInitMeta syncInitMeta = id2SyncInitMeta.get(identifier);
      if (masterSource.hasSyncMeta()) {
        SyncMeta syncMeta = masterSource.getSyncMeta();
        logger.info("Override syncer remembered position with config in file {}, watch out", syncMeta);
        syncInitMeta = new BinlogInfo(syncMeta.getBinlogFilename(), syncMeta.getBinlogPosition());
      }
      registrant.addDatasource(masterSource, syncInitMeta, masterSource.getType());
    }
    this.ackConfig = ackConfig;
  }

  public Ack getAck() {
    return ack;
  }

  public void start() throws IOException {
    ScheduledExecutorService scheduled = Executors
        .newScheduledThreadPool(1, new NamedThreadFactory("syncer-ack"));
    if (registrant.register()) {
      scheduled.scheduleAtFixedRate(new PositionFlusher(ack), 0, ackConfig.getFlushPeriod(), ackConfig.getUnit());
    } else {
      logger.warn("Fail to register");
    }
  }

  @Override
  public Set<MasterSource> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMasterSet();
  }
}
