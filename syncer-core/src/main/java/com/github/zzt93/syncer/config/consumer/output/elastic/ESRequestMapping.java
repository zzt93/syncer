package com.github.zzt93.syncer.config.consumer.output.elastic;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
@Getter
@Setter
public class ESRequestMapping {

  private int retryOnUpdateConflict = 0;
  private boolean upsert = false;

  ESRequestMapping() {
  }

  public void setRetryOnUpdateConflict(int retryOnUpdateConflict) {
    if (retryOnUpdateConflict < 0) {
      throw new InvalidConfigException(
          "retry-on-update-conflict is set a invalid value: " + retryOnUpdateConflict);
    }
    this.retryOnUpdateConflict = retryOnUpdateConflict;
  }

}
