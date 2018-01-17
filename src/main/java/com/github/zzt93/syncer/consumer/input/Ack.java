package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.syncer.SyncerMysql;
import com.github.zzt93.syncer.producer.input.connect.BinlogInfo;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt TODO 18/1/17 change identifier to Class?
 */
public class Ack {

  private static final Logger logger = LoggerFactory.getLogger(Ack.class);

  private final String metaDir;
  @ThreadSafe(sharedBy = {"main",
      "shutdown hook"}, des = "it is fixed before shutdown hook thread start")
  private final Map<String, Path> connectorMetaPath = new HashMap<>();
  @ThreadSafe(sharedBy = {"main",
      "shutdown hook"}, des = "it is fixed before shutdown hook thread start")
  private final HashMap<String, BinlogInfo> initial = new HashMap<>();
  @ThreadSafe(sharedBy = {"shutdown hook", "syncer-filter-output"})
  private Map<String, FileBasedSet<String>> ackMap = new HashMap<>();
  private final String clientId;

  public static Ack build(String clientId, SyncerMysql syncerMysql, List<String> idList,
      HashMap<String, BinlogInfo> idBinlog) {
    Ack ack = new Ack(clientId, syncerMysql);
    for (String id : idList) {
      idBinlog.put(id, ack.addDatasource(id));
    }
    ack.ackMap = Collections.unmodifiableMap(ack.ackMap);
    return ack;
  }

  private Ack(String clientId, SyncerMysql syncerMysql) {
    this.clientId = clientId;
    this.metaDir = syncerMysql.getLastRunMetadataDir();
  }

  List<String> connectorMeta(String identifier) {
    BinlogInfo binlogInfo = this.initial.get(identifier);
    return Lists.newArrayList(binlogInfo.getBinlogFilename(), "" + binlogInfo.getBinlogPosition());
  }

  Map<String, Path> connectorMetaPath() {
    return connectorMetaPath;
  }

  private BinlogInfo addDatasource(String identifier) {
    Path path = Paths.get(metaDir, clientId, identifier);
    connectorMetaPath.put(identifier, path);
    BinlogInfo binlogInfo = new BinlogInfo();
    if (!Files.exists(path)) {
      logger.info("Last run meta file not exists, fresh run");
    } else {
      try {
        binlogInfo = recoverBinlogInfo(path, binlogInfo);
        initial.put(identifier, binlogInfo);
        ackMap.put(identifier, new FileBasedSet<>(path));
      } catch (IOException ignored) {
        logger.error("Impossible to run to here", ignored);
      }
    }
    return binlogInfo;
  }

  private BinlogInfo recoverBinlogInfo(Path path, BinlogInfo binlogInfo) throws IOException {
    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
    if (lines.size() > 0) {
      binlogInfo = new BinlogInfo(lines.get(0), Long.parseLong(lines.get(1)));
    } else {
      logger.warn("Meta file in {} crashed, take as fresh run", connectorMetaPath);
    }
    return binlogInfo;
  }

  public void append(String identifier, String dataId) {
    boolean append = ackMap.get(identifier).append(dataId);
    if (!append) {
      logger.error("Fail to append to ack log: {} {}", identifier, dataId);
    }
  }

  public void remove(String identifier, String dataId) {
    boolean remove = ackMap.get(identifier).remove(dataId);
    if (!remove) {
      logger.error("Fail to remote from ack log: {} {}", identifier, dataId);
    }
  }

  public void flush() {
    ackMap.values().forEach(FileBasedSet::flush);
  }
}
