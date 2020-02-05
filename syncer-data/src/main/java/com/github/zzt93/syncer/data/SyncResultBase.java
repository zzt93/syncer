package com.github.zzt93.syncer.data;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author zzt
 */

@Getter
@Setter
@ToString
public class SyncResultBase {

  /**
   * {@link #fields} have to use `LinkedHashMap` to be in order to support multiple dependent {@link ExtraQuery}
   */
  protected LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
  protected LinkedHashMap<String, Object> extras;
  protected HashMap<String, Object> before;

  protected SimpleEventType eventType;
  protected String repo;
  protected String entity;
  /**
   * entity primary key
   */
  protected Object id;
  protected String primaryKeyName;


}