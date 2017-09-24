package com.github.zzt93.syncer.config.pipeline.output;

import com.github.zzt93.syncer.output.mapper.JsonMapper;
import java.util.HashMap;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class DocumentMapping {

  private String index = "#{schema}";
  private String type = "#{table}";
  private String documentId = "#{id}";
  private HashMap<String, Object> fieldsMapper = new HashMap<>();

  public DocumentMapping() {
    fieldsMapper.put("anyKey", JsonMapper.ROW_FLATTEN);
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
    return fieldsMapper;
  }

  public void setFieldsMapper(HashMap<String, Object> fieldsMapper) {
    this.fieldsMapper = fieldsMapper;
  }
}
