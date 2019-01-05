package com.github.zzt93.syncer;


/**
 * @author zzt Avoid to start thread in constructor which is thread-unsafe cause unsafe publication
 */
public interface Starter {

  Starter start() throws Exception;

  void close() throws Exception;

  void registerToHealthCenter();
}
