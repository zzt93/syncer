package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncEvent;

import java.util.List;

/**
 * @author zzt
 */
public interface OutputChannel {

    boolean output(SyncEvent event);

    boolean output(List<SyncEvent> batch);
}
