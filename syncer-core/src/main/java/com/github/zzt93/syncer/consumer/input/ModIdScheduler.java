package com.github.zzt93.syncer.consumer.input;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;

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
    // precondition: syncData#id is integer type
    Number dataId = (Number) syncData.getId();
    long id;
    try {
      id = dataId.longValue();
    } catch (Exception e) {
      String msg = "Invalid [scheduler] config for {}, [id] is not Long nor Integer";
      logger.error(msg, syncData, e);
      throw new InvalidConfigException(msg);
    }
    deques[(int) (id % size)].addLast(syncData);
    return true;
  }
}
