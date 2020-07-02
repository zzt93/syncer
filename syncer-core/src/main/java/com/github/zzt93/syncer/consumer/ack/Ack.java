package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.common.MasterSource;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.github.zzt93.syncer.config.syncer.SyncerInputMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
  private Map<String, FileBasedMap<DataId>> ackMap = new HashMap<>();
  private final String consumerId;
  private final int outputSize;

  public static Ack build(String consumerId, SyncerInputMeta syncerInputMeta, Set<MasterSource> masterSources,
      HashMap<String, SyncInitMeta> ackConnectionId2SyncInitMeta, int outputSize) {
    Ack ack = new Ack(consumerId, syncerInputMeta, outputSize);
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

  private Ack(String consumerId, SyncerInputMeta syncerInputMeta, int outputSize) {
    this.consumerId = consumerId;
    this.metaDir = syncerInputMeta.getLastRunMetadataDir();
    this.outputSize = outputSize;
  }

  private SyncInitMeta addDatasource(String identifier, MasterSourceType sourceType) {
    Path path = Paths.get(metaDir, consumerId, identifier);

    FileBasedMap<DataId> fileBasedMap = new FileBasedMap<>(path);
    ackMap.put(identifier, fileBasedMap);

    SyncInitMeta syncInitMeta = null;
    try {
      syncInitMeta = recoverSyncInitMeta(fileBasedMap, sourceType, syncInitMeta);
    } catch (IOException e) {
      logger.error("Impossible to run to here", e);
    }
    return syncInitMeta;
  }

  private SyncInitMeta recoverSyncInitMeta(FileBasedMap<DataId> fileBasedMap,
                                           MasterSourceType sourceType, SyncInitMeta syncInitMeta) throws IOException {
    byte[] bytes = fileBasedMap.readData();
    if (bytes.length > 0) {
      try {
        String data = new String(bytes, StandardCharsets.UTF_8);
        switch (sourceType) {
          case MySQL:
            syncInitMeta = DataId.fromDataId(data);
            break;
          case Mongo:
            syncInitMeta = DataId.fromMongoDataId(data);
            break;
        }
      } catch (Exception e) {
        logger.warn("Meta file in {} crashed, take as fresh run", fileBasedMap);
      }
    } else {
      logger.warn("Meta file in {} crashed, take as fresh run", fileBasedMap);
    }
    return syncInitMeta;
  }

  /**
   * append `outputSize` at the beginning of consumer
   */
  public void append(String identifier, DataId dataId) {
    if (ackMap.get(identifier).append(dataId, outputSize)) {
      logger.debug("Append {} {} to ack log", identifier, dataId);
    } else {
      logger.error("Already append: {} {}", identifier, dataId);
    }
  }

  /**
   * remove one when ack is received from a output
   */
  public void remove(String identifier, DataId dataId) {
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
    for (FileBasedMap<? super DataId> map : ackMap.values()) {
      res = map.flush() && res;
    }
    return res;
  }
}
