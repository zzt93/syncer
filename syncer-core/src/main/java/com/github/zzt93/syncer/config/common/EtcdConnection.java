package com.github.zzt93.syncer.config.common;

/**
 * @author zzt
 */
public class EtcdConnection extends HttpConnection {

  private String instanceId;
  private String consumerId;
  private String inputIdentifier;


  public EtcdConnection setInstanceId(String instanceId) {
    this.instanceId = instanceId;
    return this;
  }

  public EtcdConnection setConsumerId(String consumerId) {
    this.consumerId = consumerId;
    return this;
  }

  public EtcdConnection setInputIdentifier(String inputIdentifier) {
    this.inputIdentifier = inputIdentifier;
    return this;
  }

  public String getKey() {
    return instanceId + "/" + consumerId + "/" + inputIdentifier;
  }


}
