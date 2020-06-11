package com.github.zzt93.syncer.config.syncer;


import lombok.Data;

/**
 * @author zzt
 */
@Data
public class SyncerOutputMeta {

  private String failureLogDir = "./failure/";
  private int worker = 2;
  private int capacity = 100000;

}
