package com.github.zzt93.syncer.config.common;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zzt
 */
@Setter
@Getter
public class HttpConnection extends Connection {

  private String path = "";

  @Override
  public String toConnectionUrl(String ignored) {
    return "http://" + super.toConnectionUrl(null) + "/" + path;
  }
}
