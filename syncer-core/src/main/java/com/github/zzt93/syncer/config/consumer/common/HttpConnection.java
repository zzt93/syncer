package com.github.zzt93.syncer.config.consumer.common;

/**
 * @author zzt
 */
public class HttpConnection extends Connection {

  private String path = "";

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public String toConnectionUrl(String ignored) {
    return "http://" + super.toConnectionUrl(null) + "/" + path;
  }
}
