package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.common.SchemaUnavailableException;
import com.github.zzt93.syncer.config.producer.InvalidPasswordException;
import com.github.zzt93.syncer.producer.dispatch.mysql.MysqlDispatcher;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.meta.Consumer;
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
import java.util.*;
import java.util.concurrent.TimeUnit;
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
      String file, ConsumerRegistry registry)
      throws IOException, SchemaUnavailableException {
    String password = connection.getPassword();
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    connectorIdentifier = connection.connectionIdentifier();

    BinlogInfo remembered = configLogClient(connection, password, registry);
    listener = configEventListener(connection, registry, remembered);
    this.file = file;
  }

  private BinlogInfo configLogClient(MysqlConnection connection, String password,
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
      logger.info("No binlog info provided by consumer, connect to oldest binlog");
      setOldestLog();
    }
    return binlogInfo;
  }

  private SyncListener configEventListener(MysqlConnection connection, ConsumerRegistry registry,
      BinlogInfo remembered)
      throws SchemaUnavailableException {
    HashMap<Consumer, ProducerSink> consumerSink = registry.outputSink(connection);
    HashMap<ConsumerSchemaMeta, ProducerSink> sinkMap;
    try {
      sinkMap = new ConsumerSchemaMeta.MetaDataBuilder(connection, consumerSink).build();
    } catch (SQLException e) {
      throw new SchemaUnavailableException(e);
    }
    SyncListener eventListener = new SyncListener(new MysqlDispatcher(sinkMap, this.binlogInfo, remembered));
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
    EventDeserializer eventDeserializer = SyncDeserializer.defaultDeserialzer();
    // TODO 18/6/3 change to auto detect checksum type
    eventDeserializer.setChecksumType(ChecksumType.CRC32);
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
      } catch (InvalidBinlogException e) {
        oldestLog(e);
      } catch (DupServerIdException | EOFException e) {
        logger.warn("Dup serverId {} detected, reconnect again", client.getServerId());
        client.setServerId(random.nextInt(Integer.MAX_VALUE));
      } catch (IOException e) {
        logger.error("Fail to connect to master. Reconnect in {}(s)", sleepInSecond, e);
        try {
          sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
          TimeUnit.SECONDS.sleep(sleepInSecond);
        } catch (InterruptedException e1) {
          logger.error("Interrupt mysql {}", connectorIdentifier, e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    logger.info("[Shutting down] Mysql master connector closed");
  }

  private void oldestLog(InvalidBinlogException e) {
    logger.error("Invalid binlog file info {}@{}, reconnect to oldest binlog",
        client.getBinlogFilename(), client.getBinlogPosition(), e);
    setOldestLog();
  }

  private void setOldestLog() {
    // fetch oldest binlog file, but can't ensure no data loss if syncer is closed too long
    client.setBinlogFilename("");
    // have to reset it to avoid exception
    client.setBinlogPosition(0);
  }

  private void latestLog(InvalidBinlogException e) {
    logger.error("Invalid binlog file info {}@{}, reconnect to latest binlog",
        client.getBinlogFilename(), client.getBinlogPosition(), e);
    client.setBinlogFilename(null);
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
