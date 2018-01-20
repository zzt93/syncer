package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.common.data.SyncInitMeta;

/**
 * @author zzt
 */
public class DocId implements SyncInitMeta<DocId> {

  @Override
  public int compareTo(DocId o) {
    return 0;
  }
}
