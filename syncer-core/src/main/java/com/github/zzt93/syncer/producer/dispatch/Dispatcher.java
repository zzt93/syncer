package com.github.zzt93.syncer.producer.dispatch;

import com.github.zzt93.syncer.data.SimpleEventType;

/**
 * @author zzt
 */
public interface Dispatcher {

  boolean dispatch(SimpleEventType simpleEventType, Object... data);

}
