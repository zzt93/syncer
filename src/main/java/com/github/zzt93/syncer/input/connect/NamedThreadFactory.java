package com.github.zzt93.syncer.input.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zzt
 */
public class NamedThreadFactory implements ThreadFactory {
    private static final AtomicLong count = new AtomicLong(1);
    private static Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, "syncer-tmp-" + count.getAndAdd(1L));
    }
}
