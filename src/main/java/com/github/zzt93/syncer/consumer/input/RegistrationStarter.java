package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.input.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class RegistrationStarter implements Starter<PipelineInput, Set<MasterSource>> {

  private final Logger logger = LoggerFactory.getLogger(RegistrationStarter.class);
  private Registrant registrant;
  private Ack ack;

  public RegistrationStarter(PipelineInput pipelineInput, SyncerInput input,
      ConsumerRegistry consumerRegistry, String consumerId,
      BlockingDeque<SyncData> filterInput)  {
    registrant = new Registrant(consumerId, consumerRegistry, filterInput);
    HashMap<String, SyncInitMeta> id2SyncInitMeta = new HashMap<>();
    Set<MasterSource> masterSet = pipelineInput.getMasterSet();
    ack = Ack.build(consumerId, input.getInputMeta(), masterSet, id2SyncInitMeta);
    for (MasterSource masterSource : masterSet) {
      String identifier = masterSource.getConnection().connectionIdentifier();
      SyncInitMeta syncInitMeta = id2SyncInitMeta.get(identifier);
      registrant.addDatasource(masterSource, syncInitMeta, masterSource.getType());
    }
  }

  public Ack getAck() {
    return ack;
  }

  public void start() throws IOException {
    ScheduledExecutorService scheduled = Executors
        .newScheduledThreadPool(1, new NamedThreadFactory("syncer-ack"));
    if (registrant.register()) {
      scheduled.scheduleAtFixedRate(new PositionFlusher(ack), 0, 100, TimeUnit.MILLISECONDS);
    } else {
      logger.warn("Fail to register");
    }
  }

  @Override
  public Set<MasterSource> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMasterSet();
  }
}
