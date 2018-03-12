package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.config.pipeline.InvalidPasswordException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.producer.dispatch.mysql.MysqlDispatcher;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchema;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class MysqlMasterConnector implements MasterConnector {

  private final static Random random = new Random();
  private final String connectorIdentifier;
  private final int maxRetry;
  private Logger logger = LoggerFactory.getLogger(MysqlMasterConnector.class);
  private BinaryLogClient client;
  private AtomicReference<BinlogInfo> binlogInfo = new AtomicReference<>();

  public MysqlMasterConnector(MysqlConnection connection,
      ConsumerRegistry registry, int maxRetry)
      throws IOException, SchemaUnavailableException {
    this.maxRetry = maxRetry;
    String password = FileUtil.readAll(connection.getPasswordFile());
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    connectorIdentifier = connection.initIdentifier();

    configLogClient(connection, password, registry);
    configEventListener(connection, registry);
  }

  private void configLogClient(MysqlConnection connection, String password,
      ConsumerRegistry registry) throws IOException {
    client = new BinaryLogClient(connection.getAddress(), connection.getPort(),
        connection.getUser(), password);
    client.registerLifecycleListener(new LogLifecycleListener());
    client.setEventDeserializer(SyncDeserializer.defaultDeserialzer());
    client.setServerId(random.nextInt(Integer.MAX_VALUE));
    client.setSSLMode(SSLMode.DISABLED);
    BinlogInfo binlogInfo = registry.votedBinlogInfo(connection);
    if (!binlogInfo.isEmpty()) {
      client.setBinlogFilename(binlogInfo.getBinlogFilename());
      client.setBinlogPosition(binlogInfo.getBinlogPosition());
    } else {
      logger.info("No binlog info provided by consumer, connect to latest binlog");
    }

  }

  private void configEventListener(MysqlConnection connection, ConsumerRegistry registry)
      throws SchemaUnavailableException {
    IdentityHashMap<ConsumerSchema, OutputSink> schemasConsumerMap = registry.outputSink(connection);
    IdentityHashMap<ConnectionSchemaMeta, OutputSink> sinkHashMap;
    try {
      sinkHashMap = new ConnectionSchemaMeta.MetaDataBuilder(connection, schemasConsumerMap)
          .build();
    } catch (SQLException e) {
      logger.error("Fail to connect to master to retrieve schema metadata", e);
      throw new SchemaUnavailableException(e);
    }
    SyncListener eventListener = new SyncListener(new MysqlDispatcher(sinkHashMap, binlogInfo));
    client.registerEventListener(eventListener);
    client.registerEventListener((event) -> binlogInfo
        .set(new BinlogInfo(client.getBinlogFilename(), client.getBinlogPosition())));
  }

  @Override
  public void run() {
    Thread.currentThread().setName(connectorIdentifier);
    long sleepInSecond = 1;
    for (int i = 0; i < maxRetry; i++) {
      try {
        // this method is blocked
        client.connect();
      } catch (InvalidBinlogException e) {
        logger.warn("Invalid binlog file info {}@{}, reconnect to older binlog",
            client.getBinlogFilename(), client.getBinlogPosition(), e);
        i = 0;
//        client.setBinlogFilename(""); not fetch oldest log
        client.setBinlogFilename(null);
      } catch (IOException e) {
        logger.error("Fail to connect to master. Reconnect to master in {}, left retry time: {}",
            sleepInSecond, maxRetry - i - 1, e);
        try {
          sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
          TimeUnit.SECONDS.sleep(sleepInSecond);
        } catch (InterruptedException ignored) {
          logger.error("", ignored);
        }
      }
    }
    logger.error("Max try exceeds, fail to connect");
  }
}
