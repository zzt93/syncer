package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.thread.StarterFuture;

/**
 * @author zzt Avoid to start thread in constructor which is thread-unsafe cause unsafe publication
 */
public interface Starter<I, O> {

  StarterFuture start() throws Exception;

  O fromPipelineConfig(I input) throws Exception;

  void close() throws Exception;

}
