package com.github.zzt93.syncer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.zzt93.syncer.common.SchemaMeta;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.common.util.NetworkUtil;
import com.github.zzt93.syncer.config.pipeline.InvalidPasswordException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.config.syncer.SyncerMysql;
import com.github.zzt93.syncer.input.filter.InputEnd;
import com.github.zzt93.syncer.input.filter.InputFilter;
import com.github.zzt93.syncer.input.filter.InputStart;
import com.github.zzt93.syncer.input.filter.RowFilter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class MasterConnector implements Runnable {

  private final static Random random = new Random();
  private final String remote;
  private final Path connectorMetaPath;
  private Logger logger = LoggerFactory.getLogger(MasterConnector.class);
  private BinaryLogClient client;

  public MasterConnector(MysqlConnection connection, Schema schema,
      BlockingQueue<SyncData> queue, SyncerMysql mysqlMastersMeta)
      throws IOException, SchemaUnavailableException {
    String password = FileUtil.readAll(connection.getPasswordFile());
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    String connectorIdentifier = connection.getAddress() + ":" + connection.getPort();
    connectorMetaPath = Paths.get(mysqlMastersMeta.getLastRunMetaPath(), connectorIdentifier);

    client = new BinaryLogClient(connection.getAddress(), connection.getPort(),
        connection.getUser(), password);
    client.registerLifecycleListener(new LogLifecycleListener());
    client.setEventDeserializer(SyncDeserializer.defaultDeserialzer());
    client.setServerId(random.nextInt(Integer.MAX_VALUE));
    client.setSSLMode(SSLMode.DISABLED);
    if (!Files.exists(connectorMetaPath)) {
      logger.info("Last run meta file not exists, fresh run");
    } else {
      List<String> lines = Files.readAllLines(connectorMetaPath, StandardCharsets.UTF_8);
      if (lines.size() == 2) {
        client.setBinlogFilename(lines.get(0));
        client.setBinlogPosition(Long.parseLong(lines.get(1)));
      } else {
        logger.error("Invalid last run meta file, is it crash? Take it as fresh run");
      }
    }

    List<InputFilter> filters = new ArrayList<>();
    SchemaMeta schemaMeta = null;
    if (schema != null) {
      try {
        schemaMeta = new SchemaMeta.MetaDataBuilder(connection, schema).build();
        filters.add(new RowFilter(schemaMeta));
      } catch (SQLException e) {
        logger.error("Fail to connect to master to retrieve schema metadata", e);
        throw new SchemaUnavailableException(e);
      }
    }
    client.registerEventListener(
        new SyncListener(new InputStart(schemaMeta), filters, new InputEnd(), queue));

    remote = NetworkUtil.toIp(connection.getAddress()) + ":" + connection.getPort();

  }

  Path connectorMetaPath() {
    return connectorMetaPath;
  }

  List<String> connectorMeta() {
    return Lists.newArrayList(client.getBinlogFilename(), "" + client.getBinlogPosition());
  }


  @Override
  public void run() {
    Thread.currentThread().setName(remote);
    for (int i = 0; i < 3; i++) {
      try {
        client.connect();
      } catch (IOException e) {
        logger.error("Fail to connect to master", e);
      }
    }
    logger.error("Max try exceeds, fail to connect");
  }
}
