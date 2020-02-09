package com.github.zzt93.syncer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
@Data
public class Filter {

  private static final Filter id = new Filter(null, null);
  private final String docKeyName;
  private final String fieldKeyName;
  private Filter next;

  public static Filter of(String docKeyName, String fieldKeyName) {
    return new Filter(docKeyName, fieldKeyName);
  }

  public static Filter id(String fieldKeyName) {
    return new Filter(null, fieldKeyName);
  }

  public static Filter id() {
    return id;
  }

  public Filter and(String docKeyName, String fieldKeyName) {
    Filter of = of(docKeyName, fieldKeyName);
    next = of;
    return of;
  }

  public Filter next() {
    return next;
  }

  public boolean isId() {
    return docKeyName == null;
  }

  public boolean fieldUseId() {
    return fieldKeyName == null;
  }

  public String getDocKeyNameOrDefault(String defaultName) {
    return isId() ? defaultName : docKeyName;
  }

  public Object getFieldValue(SyncData data) {
    return fieldUseId() ? data.getId() : data.getField(fieldKeyName);
  }
}
