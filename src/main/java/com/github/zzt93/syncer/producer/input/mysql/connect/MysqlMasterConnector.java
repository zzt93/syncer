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
import com.github.zzt93.syncer.config.pipeline.InvalidPasswordException;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.common.SchemaUnavailableException;
import com.github.zzt93.syncer.producer.dispatch.mysql.MysqlDispatcher;
import com.github.zzt93.syncer.producer.input.MasterConnector;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConnectionSchemaMeta;
import com.github.zzt93.syncer.producer.input.mysql.meta.ConsumerSchema;
import com.github.zzt93.syncer.producer.output.OutputSink;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private final SyncListener listener;
  private final String file;
  private Logger logger = LoggerFactory.getLogger(MysqlMasterConnector.class);
  private BinaryLogClient client;
  private AtomicReference<BinlogInfo> binlogInfo = new AtomicReference<>();

  public MysqlMasterConnector(MysqlConnection connection,
      String file, ConsumerRegistry registry, int maxRetry)
      throws IOException, SchemaUnavailableException {
    this.maxRetry = maxRetry;
    String password = connection.getPassword();
    if (StringUtils.isEmpty(password)) {
      throw new InvalidPasswordException(password);
    }

    connectorIdentifier = connection.initIdentifier();

    configLogClient(connection, password, registry);
    listener = configEventListener(connection, registry);
    this.file = file;
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

  private SyncListener configEventListener(MysqlConnection connection, ConsumerRegistry registry)
      throws SchemaUnavailableException {
    IdentityHashMap<ConsumerSchema, OutputSink> schemasConsumerMap = registry
        .outputSink(connection);
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
    return eventListener;
  }

  private long consumeFile(SyncListener listener, String fileOrDir) {
    logger.info("Consuming the old binlog from {}", fileOrDir);
    long position = 0;
    Path path = Paths.get(fileOrDir);
    List<Path> files = Collections.singletonList(path);
    if (Files.isDirectory(path)) {
      logger.warn("Consuming binlog under {} in alphabetical order! Be careful.", path);
      try (Stream<Path> s = Files.list(path)) {
        files = s.sorted(Comparator.comparing(Path::getFileName))
            .collect(Collectors.toList());
      } catch (IOException e1) {
        logger.error("Fail to read file under {}", path, e1);
        return position;
      }
    }
    EventDeserializer eventDeserializer = SyncDeserializer.defaultDeserialzer();
    // TODO 18/6/3 change to auto detect checksum type
    eventDeserializer.setChecksumType(ChecksumType.CRC32);
    Event e;
    for (Path file : files) {
      try (BinaryLogFileReader reader = new BinaryLogFileReader(
          FileUtil.getResource(file.toString()).getInputStream(), eventDeserializer)) {
        while ((e = reader.readEvent()) != null) {
          binlogInfo
              .set(new BinlogInfo(file.getFileName().toString(), ((EventHeaderV4) e.getHeader()).getPosition()));
          listener.onEvent(e);
          position = ((EventHeaderV4) e.getHeader()).getNextPosition();
        }
      } catch (Exception ex) {
        logger.error("Fail to read from {}", file, ex);
      }
      logger.info("Finished consuming the old binlog from {}", file);
    }
    return position;
  }

  @Override
  public void loop() {
    if (file != null) {
      long position = consumeFile(listener, file);
      logger.info("Continue read binlog from server using {}@{}", file, position);
      client.setBinlogFilename(file);
      client.setBinlogPosition(position);
    }
    Thread.currentThread().setName(connectorIdentifier);
    long sleepInSecond = 1;
    for (int i = 0; i < maxRetry; i++) {
      try {
        // this method is blocked
        client.connect();
      } catch (InvalidBinlogException e) {
        logger.error("Invalid binlog file info {}@{}, reconnect to latest binlog",
            client.getBinlogFilename(), client.getBinlogPosition(), e);
        // fetch oldest binlog file, but can't ensure no data loss if syncer is closed too long
        client.setBinlogFilename("");
        // have to reset it to avoid exception
        client.setBinlogPosition(0);
        i = 0;
      } catch (DupServerIdException | EOFException e) {
        logger.warn("Dup serverId {} detected, reconnect again", client.getServerId());
        client.setServerId(random.nextInt(Integer.MAX_VALUE));
        i = 0;
      } catch (IOException e) {
        logger.error("Fail to connect to master. Reconnect to master in {}, left retry time: {}",
            sleepInSecond, maxRetry - i - 1, e);
        try {
          sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
          TimeUnit.SECONDS.sleep(sleepInSecond);
        } catch (InterruptedException e1) {
          logger.error("Interrupt mysql {}", connectorIdentifier, e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    logger.error("Max try exceeds, fail to connect");
  }
}
