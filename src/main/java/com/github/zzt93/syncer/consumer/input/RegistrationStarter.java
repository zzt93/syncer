package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.input.MysqlMaster;
import com.github.zzt93.syncer.config.pipeline.input.PipelineInput;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class RegistrationStarter implements Starter<PipelineInput, Set<MysqlMaster>> {

  private final Logger logger = LoggerFactory.getLogger(RegistrationStarter.class);
  private Registrant registrant;
  private Ack ack;

  public RegistrationStarter(PipelineInput pipelineInput, SyncerInput input,
      ConsumerRegistry consumerRegistry, int consumerId,
      BlockingDeque<SyncData> filterInput)  {
    String clientId = getClientId(consumerId);
    registrant = new Registrant(clientId, consumerRegistry, filterInput);
    List<String> idList = pipelineInput.getMysqlMasterSet().stream()
        .map(mysqlMaster -> mysqlMaster.getConnection().initIdentifier()).collect(
            Collectors.toList());
    HashMap<String, BinlogInfo> id2Binlog = new HashMap<>();
    ack = Ack.build(clientId, input.getMysqlMasters(), idList, id2Binlog);
    for (MysqlMaster mysqlMaster : pipelineInput.getMysqlMasterSet()) {
      String identifier = mysqlMaster.getConnection().initIdentifier();
      BinlogInfo binlogInfo = id2Binlog.get(identifier);
      registrant.addDatasource(mysqlMaster, binlogInfo);
    }
  }

  public Ack getAck() {
    return ack;
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

  @Override
  public Set<MysqlMaster> fromPipelineConfig(PipelineInput pipelineInput) {
    return pipelineInput.getMysqlMasterSet();
  }
}
