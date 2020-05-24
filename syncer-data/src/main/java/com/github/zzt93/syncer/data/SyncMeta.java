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

  public boolean isWrite() {
    return getEventType() == SimpleEventType.WRITE;
  }

  public boolean isUpdate() {
    return getEventType() == SimpleEventType.UPDATE;
  }

  public boolean isDelete() {
    return getEventType() == SimpleEventType.DELETE;
  }

}