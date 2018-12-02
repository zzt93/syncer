package com.github.zzt93.syncer.producer.dispatch;

/**
 * @author zzt
 */
public interface Dispatcher {

  boolean dispatch(Object... data);

}
