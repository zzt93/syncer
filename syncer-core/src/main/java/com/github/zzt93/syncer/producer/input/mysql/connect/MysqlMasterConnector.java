package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.producer.InvalidPasswordException;
import com.github.zzt93.syncer.producer.dispatch.mysql.MysqlDispatcher;
import com.github.zzt93.syncer.producer.input.Consumer;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchemaMeta;
import com.github.zzt93.syncer.producer.output.ProducerSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zzt
 */
public class MysqlMasterConnector implements MasterConnector {

  private final static Random random = new Random();
  private final String connectorIdentifier;
  private final SyncListener listener;
  private final String file;
  private Logger logger = LoggerFactory.getLogger(MysqlMasterConnector.class);
  private BinaryLogClient client;
  private AtomicReference<BinlogInfo> binlogInfo = new AtomicReference<>();

  public MysqlMasterConnector(MysqlConnection connection,
                              String file, ConsumerRegistry registry, boolean onlyUpdated)
      throws IOException, SchemaUnavailableException {
    String password = connection.getPassword();
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    connectorIdentifier = connection.connectionIdentifier();

    configLogClient(connection, password, registry);
    listener = configEventListener(connection, registry, onlyUpdated);
    this.file = file;
  }

  private void configLogClient(MysqlConnection connection, String password,
                               ConsumerRegistry registry) throws IOException {
    client = new BinaryLogClient(connection.getAddress(), connection.getPort(),
        connection.getUser(), password);
    client.registerLifecycleListener(new LogLifecycleListener());
    client.setEventDeserializer(SyncDeserializer.defaultDeserializer());
    client.setServerId(random.nextInt(Integer.MAX_VALUE));
    client.setSSLMode(SSLMode.DISABLED);
    BinlogInfo binlogInfo = registry.votedBinlogInfo(connection);
    logger.info("Producer[{}] connect to {}", connectorIdentifier, binlogInfo);
    client.setBinlogFilename(binlogInfo.getBinlogFilename());
    client.setBinlogPosition(binlogInfo.getBinlogPosition());
  }

  private SyncListener configEventListener(MysqlConnection connection, ConsumerRegistry registry,
                                           boolean onlyUpdated)
      throws SchemaUnavailableException {
    HashMap<Consumer, ProducerSink> consumerSink = registry.outputSink(connection);
    HashMap<ConsumerSchemaMeta, ProducerSink> sinkMap;
    try {
      sinkMap = new ConsumerSchemaMeta.MetaDataBuilder(connection, consumerSink).build();
    } catch (SQLException e) {
      throw new SchemaUnavailableException(e);
    }
    SyncListener eventListener = new SyncListener(new MysqlDispatcher(sinkMap, this.binlogInfo, onlyUpdated), connection);
    // Order of listener: client has the current event position (not next),
    // so first have it, then use it in SyncListener
    client.registerEventListener((event) -> this.binlogInfo
        .set(new BinlogInfo(client.getBinlogFilename(), client.getBinlogPosition())));
    client.registerEventListener(eventListener);
    return eventListener;
  }

  private void consumeFile(SyncListener listener, String fileOrDir) {
    logger.info("Consuming the old binlog from {}", fileOrDir);
    Path path = Paths.get(fileOrDir);
    List<Path> files = Collections.singletonList(path);
    if (Files.isDirectory(path)) {
      logger.warn("Consuming binlog under {} in numeric order of binlog suffix! Be careful.", path);
      try (Stream<Path> s = Files.list(path)) {
        files = s.sorted(Comparator.comparingInt((p) -> {
          String filename = p.getFileName().toString();
          return BinlogInfo.checkFilename(filename);
        })).collect(Collectors.toList());
      } catch (IOException e) {
        logger.error("Fail to read file under {}", path, e);
        throw new InvalidConfigException("Invalid producer.file config");
      }
    }
    EventDeserializer eventDeserializer = SyncDeserializer.defaultDeserializer();
    Event e;
    for (Path file : files) {
      logger.info("Consuming the binlog {}", file);
      try (BinaryLogFileReader reader = new BinaryLogFileReader(
          FileUtil.getResource(file.toString()).getInputStream(), eventDeserializer)) {
        while ((e = reader.readEvent()) != null) {
          binlogInfo
              .set(new BinlogInfo(file.getFileName().toString(), ((EventHeaderV4) e.getHeader()).getPosition()));
          listener.onEvent(e);
        }
      } catch (Exception ex) {
        logger.error("Fail to read from {}", file, ex);
        throw new InvalidConfigException("Invalid producer.file config");
      }
      logger.info("Finished consuming the old binlog from {}", file);
    }
  }

  @Override
  public void loop() {
    Thread.currentThread().setName(connectorIdentifier);

    if (file != null) {
      consumeFile(listener, file);
      BinlogInfo binlogInfo = this.binlogInfo.get();
      logger.info("Continue read binlog from server using {}@{}", binlogInfo.getBinlogFilename(),
          binlogInfo.getBinlogPosition());
      client.setBinlogFilename(binlogInfo.getBinlogFilename());
      client.setBinlogPosition(binlogInfo.getBinlogPosition());
    }
    long sleepInSecond = 1;
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // this method is blocked
        client.connect();
        logger.info("[Shutting down] Closing connection to mysql master");
        return;
        // TODO 2020/4/28 catch MismatchedSchemaException
      } catch (InvalidBinlogException e) {
        oldestLog(e);
      } catch (DupServerIdException | EOFException e) {
        logger.warn("Dup serverId {} detected, reconnect again", client.getServerId());
        client.setServerId(random.nextInt(Integer.MAX_VALUE));
      } catch (IOException e) {
        logger.error("Fail to connect to master. Reconnect in {}(s)", sleepInSecond, e);
        sleepInSecond = FallBackPolicy.POW_2.sleep(sleepInSecond);
      }
    }
    logger.info("[Shutting down] Mysql master connector closed");
  }

  private void oldestLog(InvalidBinlogException e) {
    logger.error("Invalid binlog file info {}@{}, reconnect to earliest binlog",
        client.getBinlogFilename(), client.getBinlogPosition(), e);
    BinlogInfo earliest = BinlogInfo.earliest;
    client.setBinlogFilename(earliest.getBinlogFilename());
    client.setBinlogPosition(earliest.getBinlogPosition());
  }


  @Override
  public void close() {
    try {
      client.disconnect();
    } catch (IOException e) {
      logger.error("[Shutting down] Fail to disconnect", e);
      return;
    }
    MasterConnector.super.close();
  }
}
