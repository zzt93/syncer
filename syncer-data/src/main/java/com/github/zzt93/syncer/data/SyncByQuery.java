package com.github.zzt93.syncer.data;


/**
 * update(set field)/delete by query
 */
public interface SyncByQuery {

  /**
   * Usually used to update/delete by foreign key column, will {@link SyncData#setId(Object)} to null
   * @param syncWithCol the column name in target repo
   * @param value the value to use
   */
  SyncByQuery syncBy(String syncWithCol, Object value);

  String toString();

}
