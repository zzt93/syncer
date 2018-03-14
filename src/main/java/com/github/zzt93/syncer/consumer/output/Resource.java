package com.github.zzt93.syncer.consumer.output;

import java.io.IOException;

/**
 * @author zzt
 */
public interface Resource {

  void cleanup() throws IOException;

}
