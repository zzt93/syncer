package com.github.zzt93.syncer.config.pipeline.output;

/**
 * @author zzt
 */
public class QueryMapping {

  private Boolean supportAllByQuery = false;
  private Boolean supportIndexByQuery = false;
  private Boolean supportUpdateByQuery = false;
  private Boolean supportDeleteByQuery = false;

  public Boolean getSupportAllByQuery() {
    return supportAllByQuery;
  }

  public void setSupportAllByQuery(Boolean supportAllByQuery) {
    this.supportAllByQuery = supportAllByQuery;
  }

  public Boolean getSupportIndexByQuery() {
    return supportIndexByQuery;
  }

  public void setSupportIndexByQuery(Boolean supportIndexByQuery) {
    this.supportIndexByQuery = supportIndexByQuery;
  }

  public Boolean getSupportDeleteByQuery() {
    return supportDeleteByQuery;
  }

  public void setSupportDeleteByQuery(Boolean supportDeleteByQuery) {
    this.supportDeleteByQuery = supportDeleteByQuery;
  }

  public Boolean getSupportUpdateByQuery() {
    return supportUpdateByQuery;
  }

  public void setSupportUpdateByQuery(Boolean supportUpdateByQuery) {
    this.supportUpdateByQuery = supportUpdateByQuery;
  }

}
