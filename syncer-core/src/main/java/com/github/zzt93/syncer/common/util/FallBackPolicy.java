package com.github.zzt93.syncer.common.util;

import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author zzt
 */
@Slf4j
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
  POW_2_RANDOM {
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

  POW_2_THREAD_LOCAL_RANDOM {
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

  public long next(long last) {
    return next(last, TimeUnit.SECONDS);
  }

  public abstract long max(TimeUnit unit);

  public long sleep(long last) {
    try {
      TimeUnit.SECONDS.sleep(last);
    } catch (InterruptedException e) {
      log.error("Interrupt fallback", e);
      Thread.currentThread().interrupt();
    }
    return next(last);
  }

}
