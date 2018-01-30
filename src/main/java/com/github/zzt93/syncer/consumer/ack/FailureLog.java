package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.data.SyncWrapper;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

/**
 * @author zzt
 */
public class FailureLog<T> {

  public FailureLog(Path path) {
    List<SyncWrapper<T>> recover = recover(path);
  }

  private List<SyncWrapper<T>> recover(Path path) {
    List<SyncWrapper<T>> res = new LinkedList<>();
    return res;
  }

  public boolean log(SyncWrapper<T> syncWrapper) {

    return true;
  }

}
