package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.data.SyncInitMeta;

/**
 * @author zzt
 */
public class DocId implements SyncInitMeta<DocId> {

  private final String dataId;

  public DocId(String data) {
    dataId = data;
  }

  @Override
  public int compareTo(DocId o) {
    return dataId.compareTo(o.dataId);
  }

  @Override
  public String toString() {
    return "DocId{" +
        "dataId='" + dataId + '\'' +
        '}';
  }
}
