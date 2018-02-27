package com.github.zzt93.syncer.common.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
public enum FallBackPolicy {

  POW_2 {
    @Override
    public long next(long last, TimeUnit unit) {
      return Math.min(last * 2, max(unit));
    }

    @Override
    public long max(TimeUnit unit) {
      return TimeUnit.SECONDS.convert(64, unit);
    }
  },
  /**
   * <a href="https://aws.amazon.com/cn/blogs/architecture/exponential-backoff-and-jitter/">randomized
   * exp backoff</a> policy, not thread safe
   */
  POW_2_Random {
    @Override
    public long next(long last, TimeUnit unit) {
      long min = Math.min(last * 2, max(unit));
      return min / 2 + random.nextInt((int) (min / 2));
    }

    @Override
    public long max(TimeUnit unit) {
      return TimeUnit.SECONDS.convert(64, unit);
    }
  },

  POW_2_Random_Local {
    @Override
    public long next(long last, TimeUnit unit) {
      ThreadLocalRandom safeRandom = ThreadLocalRandom.current();
      long min = Math.min(last * 2, max(unit));
      return min / 2 + safeRandom.nextLong(min / 2);
    }

    @Override
    public long max(TimeUnit unit) {
      return TimeUnit.SECONDS.convert(64, unit);
    }
  };
  static final Random random = new Random();

  public abstract long next(long last, TimeUnit unit);

  public abstract long max(TimeUnit unit);

}
