package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.output.mapper.KVMapper;
import java.util.HashMap;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class RequestMapping {

  private String index = "schema";
  private String type = "table";
  private String documentId = "id";
  private HashMap<String, Object> fieldsMapping = new HashMap<>();
  private Boolean noUseIdForIndex = false;
  private Boolean enableExtraQuery;
  private int retryOnUpdateConflict = 0;

  // TODO 17/10/16 implement
  private HashMap<String, Object> upsert = new HashMap<>();

  public RequestMapping() {
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

  public HashMap<String, Object> getFieldsMapping() {
    if (fieldsMapping.size() > 1 && fieldsMapping.containsKey(KVMapper.FAKE_KEY)) {
      fieldsMapping.remove(KVMapper.FAKE_KEY);
    }
    return fieldsMapping;
  }

  public void setFieldsMapping(HashMap<String, Object> fieldsMapping) {
    this.fieldsMapping = fieldsMapping;
  }

  public Boolean getNoUseIdForIndex() {
    return noUseIdForIndex;
  }

  public void setNoUseIdForIndex(Boolean noUseIdForIndex) {
    this.noUseIdForIndex = noUseIdForIndex;
  }

  public void setEnableExtraQuery(Boolean enableExtraQuery) {
    this.enableExtraQuery = enableExtraQuery;
  }

  public boolean getEnableExtraQuery() {
    return enableExtraQuery;
  }

  public int getRetryOnUpdateConflict() {
    return retryOnUpdateConflict;
  }

  public void setRetryOnUpdateConflict(int retryOnUpdateConflict) {
    if (retryOnUpdateConflict < 0) {
      throw new InvalidConfigException("retry-on-update-conflict is set a invalid value: "+ retryOnUpdateConflict);
    }
    this.retryOnUpdateConflict = retryOnUpdateConflict;
  }
}
