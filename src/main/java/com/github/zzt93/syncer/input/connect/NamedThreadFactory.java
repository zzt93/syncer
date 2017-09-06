package com.github.zzt93.syncer.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (r instanceof NamedJob) {
            return new Thread(r, ((NamedJob) r).getName());
        }
        logger.warn("Not the instance of NamedJob", r);
        return new Thread(r, "" + count.getAndAdd(1L));
    }
}
