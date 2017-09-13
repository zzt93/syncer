package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.MysqlRowEvent;
import com.github.zzt93.syncer.common.SyncData;

/**
 * @author zzt
 */
public class EventToSyncData implements Filter<MysqlRowEvent, SyncData> {
    @Override
    public SyncData decide(MysqlRowEvent e) {
        return new SyncData(e);
    }
}
