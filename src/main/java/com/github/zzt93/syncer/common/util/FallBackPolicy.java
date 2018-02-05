package com.github.zzt93.syncer.common.util;

import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
public enum FallBackPolicy {

  POW_2 {
    @Override
    public long next(long last, TimeUnit unit) {
      if (last < max(unit)) {
        return last * 2;
      }
      return max(unit);
    }

    @Override
    public long max(TimeUnit unit) {
      return TimeUnit.SECONDS.convert(64, unit);
    }
  };

  public abstract long next(long last, TimeUnit unit);

  public abstract long max(TimeUnit unit);

}
