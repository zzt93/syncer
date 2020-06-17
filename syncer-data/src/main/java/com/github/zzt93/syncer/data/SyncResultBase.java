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
@ToString(callSuper = true, doNotUseGetters = true)
public class SyncResultBase extends SyncMeta {

  protected LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
  protected LinkedHashMap<String, Object> extras;
  protected HashMap<String, Object> before;

}