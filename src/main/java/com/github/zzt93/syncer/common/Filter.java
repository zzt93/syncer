package com.github.zzt93.syncer.common;


import com.github.zzt93.syncer.filter.FilterJob;
import com.github.zzt93.syncer.input.connect.MasterConnector;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public interface Filter<S, T> {

  /**
   * Should be thread safe
   *
   * @see FilterJob#call()
   * @see MasterConnector#run()
   */
  @ThreadSafe
  T decide(S e);

  enum FilterRes {
    ACCEPT, DENY,
  }
}
