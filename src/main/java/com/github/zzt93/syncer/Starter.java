package com.github.zzt93.syncer;

/**
 * @author zzt
 */
public interface Starter<I, O> {

  void start() throws Exception;

  O fromPipelineConfig(I input) throws Exception;

}
