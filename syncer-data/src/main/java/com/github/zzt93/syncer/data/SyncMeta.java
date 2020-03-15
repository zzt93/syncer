package com.github.zzt93.syncer.data;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author zzt
 */

@Getter
@Setter
@ToString
public class SyncMeta {

  protected SimpleEventType eventType;
  protected String repo;
  protected String entity;
  /**
   * entity primary key
   */
  protected Object id;
  protected String primaryKeyName;


}