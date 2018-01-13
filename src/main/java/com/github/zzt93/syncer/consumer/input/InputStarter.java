package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.consumer.InputSource;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class InputStarter implements Starter<PipelineInput, Set<MysqlMaster>> {

  private final Logger logger = LoggerFactory.getLogger(InputStarter.class);
  private Registrant registrant;
  private Ack ack;

  public InputStarter(PipelineInput pipelineInput, SyncerInput input,
      ConsumerRegistry consumerRegistry, int consumerId) throws IOException {
    String clientId = getClientId(consumerId);
    registrant = new Registrant(clientId, consumerRegistry);
    ack = new Ack(clientId, input.getMysqlMasters());
    for (MysqlMaster mysqlMaster : pipelineInput.getMysqlMasterSet()) {
      String identifier = mysqlMaster.getConnection().connectionIdentifier();
      BinlogInfo binlogInfo = ack.addDatasource(identifier);
      registrant.addDatasource(mysqlMaster, binlogInfo);
    }
  }

  private String getClientId(int consumerId) {
    return "" + consumerId;
  }

  public void start() throws IOException {
    if (registrant.register()) {
      Runtime.getRuntime().addShutdownHook(new Thread(new PositionHook(ack)));
    } else {
      logger.warn("Fail to register");
    }
  }

  public List<InputSource> getInputSources() {
    return registrant.getInputSources();
  }

  @Override
  public Set<MysqlMaster> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMysqlMasterSet();
  }
}
