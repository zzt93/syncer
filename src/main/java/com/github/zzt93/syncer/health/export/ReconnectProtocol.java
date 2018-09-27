package com.github.zzt93.syncer.health.export;

import com.google.common.base.Preconditions;
import java.net.BindException;
import org.apache.coyote.http11.Http11NioProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectProtocol extends Http11NioProtocol {
  private static final Logger logger = LoggerFactory.getLogger(ReconnectProtocol.class);

  private int retry;

  /**
   * @see com.github.zzt93.syncer.config.syncer.SyncerConfig
   */
  public void setRetry(int retry) {
    Preconditions.checkArgument(retry > 0 && retry <= 16, "Invalid retry");
    this.retry = retry;
  }

  @Override
  public void start() throws Exception {
    for (int i = 0; i < retry; i++) {
      try {
        super.start();
      } catch (BindException e) {
        logger.warn("Fail to bind to {}, retry {}", getPort(), getPort()+1);
        setPort(getPort() + 1);
      }
    }
  }
}
