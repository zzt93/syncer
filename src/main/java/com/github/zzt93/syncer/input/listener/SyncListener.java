package com.github.zzt93.syncer.listener;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class SyncListener implements BinaryLogClient.EventListener {
    private Logger logger = LoggerFactory.getLogger(SyncListener.class);

    @Override
    public void onEvent(Event event) {
        logger.debug(event.toString());
        EventType eventType = event.getHeader().getEventType();
        switch (eventType){
            case WRITE_ROWS:
            case UPDATE_ROWS:
                break;
        }
    }
}
