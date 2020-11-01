package com.github.zzt93.syncer.data.es;

import com.github.zzt93.syncer.data.SyncData;
import lombok.Data;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
@Data
public class Filter {

  private static final Filter id = new Filter(null, null);
  private final ESDocKey docKeyName;
  private final SyncDataKey fieldKeyName;

  public String getDocKeyName() {
    return docKeyName.getName();
  }

  public String getFieldKeyName() {
    return fieldKeyName.getName();
  }

  public Filter next() {
    return null;
  }

  public static Filter of(ESDocKey docKeyName, SyncDataKey fieldKeyName) {
    return new Filter(docKeyName, fieldKeyName);
  }

  public static Filter esId(SyncDataKey fieldKeyName) {
    return new Filter(null, fieldKeyName);
  }

  public static Filter syncId(ESDocKey docKeyName) {
    return new Filter(docKeyName, null);
  }

  public static Filter id() {
    return id;
  }

  public boolean esUseId() {
    return docKeyName == null;
  }

  public boolean syncDataUseId() {
    return fieldKeyName == null;
  }

  public String getDocKeyNameOrDefault(String defaultName) {
    return esUseId() ? defaultName : getDocKeyName();
  }

  public Object getFieldValue(SyncData data) {
    return syncDataUseId() ? data.getId() : data.getField(getFieldKeyName());
  }

  @Override
  public String toString() {
    String es = getDocKeyNameOrDefault("es._id");
    String sync = syncDataUseId() ? "sync.id" : "sync.field[" + getFieldKeyName() + "]";
    return "Filter(" + es + " = " + sync + ")";
  }
}
