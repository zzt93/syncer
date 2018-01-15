package com.github.zzt93.syncer.producer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.zzt93.syncer.common.ConnectionSchemaMeta;
import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.common.util.NetworkUtil;
import com.github.zzt93.syncer.config.pipeline.InvalidPasswordException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.producer.dispatch.Dispatcher;
import com.github.zzt93.syncer.producer.dispatch.InputEnd;
import com.github.zzt93.syncer.producer.dispatch.RowFilter;
import com.github.zzt93.syncer.producer.input.filter.InputFilter;
import com.github.zzt93.syncer.producer.input.filter.InputPipeHead;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class MasterConnector implements Runnable {

  private final static Random random = new Random();
  private final String connectorIdentifier;
  private Logger logger = LoggerFactory.getLogger(MasterConnector.class);
  private BinaryLogClient client;
  private AtomicReference<BinlogInfo> binlogInfo = new AtomicReference<>();

  public MasterConnector(MysqlConnection connection, List<Schema> schema,
      ConsumerRegistry registry)
      throws IOException, SchemaUnavailableException {
    String password = FileUtil.readAll(connection.getPasswordFile());
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    connectorIdentifier = NetworkUtil.toIp(connection.getAddress()) + ":" + connection.getPort();

    configLogClient(connection, password, registry);
    configEventListener(connection, schema, registry);
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

  private void configEventListener(MysqlConnection connection, List<Schema> schemas,
      ConsumerRegistry registry) throws SchemaUnavailableException {
    List<InputFilter> filters = new ArrayList<>();
    ConnectionSchemaMeta connectionSchemaMeta;
    assert !schemas.isEmpty();
    try {
      connectionSchemaMeta = new ConnectionSchemaMeta.MetaDataBuilder(connection, schemas).build();
      filters.add(new RowFilter(connectionSchemaMeta));
    } catch (SQLException e) {
      logger.error("Fail to connect to master to retrieve schema metadata", e);
      throw new SchemaUnavailableException(e);
    }
    SyncListener eventListener = new SyncListener(new InputPipeHead(connectionSchemaMeta), filters,
        new InputEnd(), new Dispatcher(registry.outputSink(connection)));
    client.registerEventListener(eventListener);
    // TODO 18/1/9 remove
    client.registerEventListener((event) -> binlogInfo
        .set(new BinlogInfo(client.getBinlogFilename(), client.getBinlogPosition())));
  }

  @Override
  public void run() {
    Thread.currentThread().setName(connectorIdentifier);
    for (int i = 0; i < 5; i++) {
      try {
        // this method is blocked
        client.connect();
      } catch (InvalidBinlogException e) {
        logger.warn("Invalid binlog file info, reconnect to older binlog", e);
        i = 0;
//        client.setBinlogFilename(""); not fetch oldest log
        client.setBinlogFilename(null);
      } catch (IOException e) {
        logger.error("Fail to connect to master", e);
      }
    }
    logger.error("Max try exceeds, fail to connect");
  }
}
