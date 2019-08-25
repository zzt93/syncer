package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.config.syncer.SyncerInputMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author zzt TODO 18/1/17 change string identifier to Class?
 */
public class Ack {

  private static final Logger logger = LoggerFactory.getLogger(Ack.class);

  private final String metaDir;
  @ThreadSafe(sharedBy = {"main", "shutdown hook", "syncer-filter-output"}, des = "Thread start rule."
      + "main thread init ack before two other thread start")
  private Map<String, FileBasedMap<String>> ackMap = new HashMap<>();
  private final String clientId;
  private final int outputSize;

  public static Ack build(String clientId, SyncerInputMeta syncerInputMeta, Set<MasterSource> masterSources,
      HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta, int outputSize) {
    Ack ack = new Ack(clientId, syncerInputMeta, outputSize);
    for (MasterSource masterSource : masterSources) {
      Set<String> ids = masterSource.remoteIds();
      for (String id : ids) {
        SyncInitMeta initMeta = ack.addDatasource(id, masterSource.getType());
        if (initMeta != null) {
          ackConnectionId2SyncInitMeta.put(id, initMeta);
        }
      }
    }
    ack.ackMap = Collections.unmodifiableMap(ack.ackMap);
    return ack;
  }

  private Ack(String clientId, SyncerInputMeta syncerInputMeta, int outputSize) {
    this.clientId = clientId;
    this.metaDir = syncerInputMeta.getLastRunMetadataDir();
    this.outputSize = outputSize;
  }

  private SyncInitMeta addDatasource(String identifier, MasterSourceType sourceType) {
    Path path = Paths.get(metaDir, clientId, identifier);
    SyncInitMeta syncInitMeta = null;
    if (!Files.exists(path)) {
      logger.info("Last run meta file[{}] not exists, fresh run", path);
    } else {
      try {
        syncInitMeta = recoverSyncInitMeta(path, sourceType, syncInitMeta);
      } catch (IOException e) {
        logger.error("Impossible to run to here", e);
      }
    }
    try {
      ackMap.put(identifier, new FileBasedMap<>(path));
    } catch (IOException e) {
      logger.error("Fail to create file {}", path);
    }
    return syncInitMeta;
  }

  private SyncInitMeta recoverSyncInitMeta(Path path,
      MasterSourceType sourceType, SyncInitMeta syncInitMeta) throws IOException {
    byte[] bytes = FileBasedMap.readData(path);
    if (bytes.length > 0) {
      try {
        String data = new String(bytes, StandardCharsets.UTF_8);
        switch (sourceType) {
          case MySQL:
            syncInitMeta = IdGenerator.fromDataId(data);
            break;
          case Mongo:
            syncInitMeta = IdGenerator.fromMongoDataId(data);
            break;
        }
      } catch (Exception e) {
        logger.warn("Meta file in {} crashed, take as fresh run", path);
      }
    } else {
      logger.warn("Meta file in {} crashed, take as fresh run", path);
    }
    return syncInitMeta;
  }

  /**
   * append `outputSize` at the beginning of consumer
   */
  public void append(String identifier, String dataId) {
    ackMap.get(identifier).append(dataId, outputSize);
  }

  /**
   * remove one when ack is received from a output
   */
  public void remove(String identifier, String dataId) {
    boolean remove = false;
    try {
      remove = ackMap.get(identifier).remove(dataId, 1);
    } catch (Exception e) {
      logger.error("Fail to remove from ack log: {} {}", identifier, dataId, e);
    }
    if (remove) {
      logger.debug("Remove {} {} from ack log", identifier, dataId);
    }
  }

  public boolean flush() {
    // - add next dataId: seems hard to do
    // - set `lastRemoved` in FileBasedMap#remove
    boolean res = true;
    for (FileBasedMap<String> map : ackMap.values()) {
      res = map.flush() && res;
    }
    return res;
  }
}
