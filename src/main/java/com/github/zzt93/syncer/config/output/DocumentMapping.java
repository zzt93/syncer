package com.github.zzt93.syncer.config.output;

/**
 * Created by zzt on 9/11/17. <p> <h3></h3>
 */
public class DocumentMapping {

  private String index;
  private String type;
  private String documentId;

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

}
