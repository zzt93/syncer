package com.github.zzt93.syncer.data;


/**
 * update(set field)/delete by query
 */
public interface SyncByQuery {


  SyncByQuery filter(String syncWithCol, Object value);

  SyncByQuery updateList(String listField, Object delta);

    @Override
  String toString();

}
