package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.output.mapper.JsonMapper;
import java.util.HashMap;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class RequestMapping {

  private String index = "schema";
  private String type = "table";
  private String documentId = "id";
  private HashMap<String, Object> fieldsMapper = new HashMap<>();
  private Boolean noUseIdForIndex = false;
  private QueryMapping queryMapping = new QueryMapping();

  // TODO 17/10/16 implement
  private HashMap<String, Object> upsert = new HashMap<>();

  public RequestMapping() {
    // default value of mapper
    fieldsMapper.put(JsonMapper.FAKE_KEY, JsonMapper.ROW_FLATTEN);
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

  public HashMap<String, Object> getFieldsMapper() {
    if (fieldsMapper.size() > 1 && fieldsMapper.containsKey(JsonMapper.FAKE_KEY)) {
      fieldsMapper.remove(JsonMapper.FAKE_KEY);
    }
    return fieldsMapper;
  }

  public void setFieldsMapper(HashMap<String, Object> fieldsMapper) {
    this.fieldsMapper = fieldsMapper;
  }

  public Boolean getNoUseIdForIndex() {
    return noUseIdForIndex;
  }

  public void setNoUseIdForIndex(Boolean noUseIdForIndex) {
    this.noUseIdForIndex = noUseIdForIndex;
  }


  public QueryMapping getQueryMapping() {
    return queryMapping;
  }

  public void setQueryMapping(QueryMapping queryMapping) {
    this.queryMapping = queryMapping;
  }}
