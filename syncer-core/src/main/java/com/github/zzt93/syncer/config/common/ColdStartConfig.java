package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.ConsumerConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author zzt
 */
@ConsumerConfig("input[].cold")
@Getter
@Setter
@ToString
public class ColdStartConfig {

  /**
   * mysql filter sql
   */
  private String where;
  /**
   * mongo filter
   */
  private String find;

  private int pageSize = 10000;

  private long bytesLimit = 1024 * 1024 * 10;

}
