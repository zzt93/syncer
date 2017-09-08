package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;

import java.util.List;

/**
 * @author zzt
 */
public interface OutputChannel {

    boolean output(SyncData event);

    boolean output(List<SyncData> batch);
}
