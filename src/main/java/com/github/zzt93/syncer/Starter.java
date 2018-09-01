package com.github.zzt93.syncer;


/**
 * @author zzt Avoid to start thread in constructor which is thread-unsafe cause unsafe publication
 */
public interface Starter<I, O> {

  Starter start() throws Exception;

  O fromPipelineConfig(I input) throws Exception;

  void close() throws Exception;

  void registerToHealthCenter();
}
