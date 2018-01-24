package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.input.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.MasterSourceType;
import com.github.zzt93.syncer.config.syncer.SyncerMeta;
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
 * @author zzt TODO 18/1/17 change identifier to Class?
 */
public class Ack {

  private static final Logger logger = LoggerFactory.getLogger(Ack.class);

  private final String metaDir;
  @ThreadSafe(sharedBy = {"shutdown hook", "syncer-filter-output"})
  private Map<String, FileBasedSet<String>> ackMap = new HashMap<>();
  private final String clientId;

  public static Ack build(String clientId, SyncerMeta syncerMeta, Set<MasterSource> masterSources,
      HashMap<String, SyncInitMeta> id2SyncInitMeta) {
    Ack ack = new Ack(clientId, syncerMeta);
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

  private Ack(String clientId, SyncerMeta syncerMeta) {
    this.clientId = clientId;
    this.metaDir = syncerMeta.getLastRunMetadataDir();
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
      ackMap.put(identifier, new FileBasedSet<>(path));
    } catch (IOException e) {
      logger.error("Fail to create file {}", path);
    }
    return syncInitMeta;
  }

  private SyncInitMeta recoverSyncInitMeta(Path path,
      MasterSourceType sourceType, SyncInitMeta syncInitMeta) throws IOException {
    byte[] bytes = FileBasedSet.readData(path);
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

  public void append(String identifier, String dataId) {
    boolean append = ackMap.get(identifier).append(dataId);
    if (!append) {
      logger.error("Fail to append to ack log: {} {}", identifier, dataId);
    }
  }

  public void remove(String identifier, String dataId) {
    boolean remove = ackMap.get(identifier).remove(dataId);
    if (!remove) {
      logger.error("Fail to remove from ack log: {} {}", identifier, dataId);
    }
  }

  public void flush() {
    ackMap.values().forEach(FileBasedSet::flush);
  }
}
