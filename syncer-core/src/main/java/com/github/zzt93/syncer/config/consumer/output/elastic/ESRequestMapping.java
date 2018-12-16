package com.github.zzt93.syncer.config.consumer.output.elastic;

import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;

import java.util.LinkedHashMap;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class ESRequestMapping {

  private String index = "schema";
  private String type = "table";
  private String documentId = "id";
  private LinkedHashMap<String, Object> fieldsMapping = new LinkedHashMap<>();
  private boolean noUseIdForIndex = false;
  private boolean enableExtraQuery = false;
  private int retryOnUpdateConflict = 0;
  private boolean upsert = false;

  public ESRequestMapping() {
    // default value of mapper
    fieldsMapping.put(KVMapper.FAKE_KEY, KVMapper.ROW_FLATTEN);
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDocumentId() {
    return documentId;
  }

  public void setDocumentId(String documentId) {
    this.documentId = documentId;
  }

  public LinkedHashMap<String, Object> getFieldsMapping() {
    if (fieldsMapping.size() > 1 && fieldsMapping.containsKey(KVMapper.FAKE_KEY)) {
      fieldsMapping.remove(KVMapper.FAKE_KEY);
    }
    return fieldsMapping;
  }

  public void setFieldsMapping(LinkedHashMap<String, Object> fieldsMapping) {
    this.fieldsMapping = fieldsMapping;
  }

  public boolean getNoUseIdForIndex() {
    return noUseIdForIndex;
  }

  public void setNoUseIdForIndex(boolean noUseIdForIndex) {
    this.noUseIdForIndex = noUseIdForIndex;
  }

  public boolean getEnableExtraQuery() {
    return enableExtraQuery;
  }

  public void setEnableExtraQuery(boolean enableExtraQuery) {
    this.enableExtraQuery = enableExtraQuery;
  }

  public int getRetryOnUpdateConflict() {
    return retryOnUpdateConflict;
  }

  public void setRetryOnUpdateConflict(int retryOnUpdateConflict) {
    if (retryOnUpdateConflict < 0) {
      throw new InvalidConfigException(
          "retry-on-update-conflict is set a invalid value: " + retryOnUpdateConflict);
    }
    this.retryOnUpdateConflict = retryOnUpdateConflict;
  }

  public boolean isUpsert() {
    return upsert;
  }

  public void setUpsert(boolean upsert) {
    this.upsert = upsert;
  }
}
