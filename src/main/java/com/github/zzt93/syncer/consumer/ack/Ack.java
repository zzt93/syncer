package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.MasterSourceType;
import com.github.zzt93.syncer.config.syncer.SyncerInputMeta;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static Ack build(String clientId, SyncerInputMeta syncerInputMeta, Set<MasterSource> masterSources,
      HashMap<String, SyncInitMeta> id2SyncInitMeta) {
    Ack ack = new Ack(clientId, syncerInputMeta);
    for (MasterSource masterSource : masterSources) {
      String id = masterSource.getConnection().initIdentifier();
      SyncInitMeta initMeta = ack.addDatasource(id, masterSource.getType());
      if (initMeta != null) {
        id2SyncInitMeta.put(id, initMeta);
      }
    }
    ack.ackMap = Collections.unmodifiableMap(ack.ackMap);
    return ack;
  }

  private Ack(String clientId, SyncerInputMeta syncerInputMeta) {
    this.clientId = clientId;
    this.metaDir = syncerInputMeta.getLastRunMetadataDir();
  }

  private SyncInitMeta addDatasource(String identifier, MasterSourceType sourceType) {
    Path path = Paths.get(metaDir, clientId, identifier);
    SyncInitMeta syncInitMeta = SyncInitMeta.defaultMeta(sourceType);
    if (!Files.exists(path)) {
      logger.info("Last run meta file not exists, fresh run");
    } else {
      try {
        syncInitMeta = recoverSyncInitMeta(path, sourceType, syncInitMeta);
      } catch (IOException ignored) {
        logger.error("Impossible to run to here", ignored);
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
        String data = new String(bytes, "utf-8");
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

  public void append(String identifier, String dataId, int size) {
    boolean append = ackMap.get(identifier).append(dataId, size) == null;
    if (!append) {
      logger.error("Fail to append to ack log: {} {}", identifier, dataId);
    }
  }

  public void remove(String identifier, String dataId) {
    boolean remove = false;
    try {
      remove = ackMap.get(identifier).remove(dataId, 1) == null;
    } catch (Exception e) {
      logger.error("Fail to remove from ack log: {} {}", identifier, dataId, e);
    }
    if (remove) {
      logger.info("Remove {} {} from ack log", identifier, dataId);
    }
  }

  public void flush() {
    // TODO 18/8/20 If dataId is just removed and map is empty, add next dataId
    ackMap.values().forEach(FileBasedMap::flush);
  }
}
