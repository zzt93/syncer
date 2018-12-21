package com.github.zzt93.syncer.data;


/**
 * update(set field)/delete by query
 */
public interface SyncByQuery {


  SyncByQuery filter(String syncWithCol, Object value);

  @Override
  String toString();

}
