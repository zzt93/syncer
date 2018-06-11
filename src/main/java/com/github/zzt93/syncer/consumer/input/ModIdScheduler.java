package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import java.util.concurrent.BlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class ModIdScheduler implements EventScheduler {

  private final int size;
  private final BlockingDeque<SyncData>[] deques;
  private final Logger logger = LoggerFactory.getLogger(ModIdScheduler.class);

  ModIdScheduler(BlockingDeque<SyncData>[] deques) {
    this.deques = deques;
    size = deques.length;
  }

  @Override
  public boolean schedule(SyncData syncData) {
    // precondition: syncData#id instanceOf Long or Integer
    long id;
    try {
      Object dataId = syncData.getId();
      id = dataId instanceof Long ? (long) dataId : (int) dataId;
    } catch (Exception e) {
      logger.error("Invalid [scheduler] config for {}, [id] is not Long nor Integer", syncData, e);
      throw new InvalidConfigException("");
    }
    deques[(int) (id % size)].addLast(syncData);
    return true;
  }
}
