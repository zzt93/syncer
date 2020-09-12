package com.github.zzt93.syncer.config.syncer;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author zzt
 */
@Getter
@Setter
@ToString
public class SyncerOutputMeta {

  private String failureLogDir = "./failure/";
  private int worker = 2;
  private int capacity = 100000;

}
