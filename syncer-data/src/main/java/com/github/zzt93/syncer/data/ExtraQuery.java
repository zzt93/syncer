package com.github.zzt93.syncer.data;

/**
 * Write/Update/Delete by query <b>output channel</b>
 *
 * @see SyncByQuery
 */
public interface ExtraQuery {

  ExtraQuery setTypeName(String typeName);

  /**
   * Add condition for extraQuery
   * @see #eq(String, Object) for replace
   * @param name name of target field in repo.entity in <b>output channel</b>
   * @param value value of target field in repo.entity in <b>output channel</b>
   */
  @Deprecated
  ExtraQuery filter(String name, Object value);

  /**
   * Same as {@link #filter}
   */
  ExtraQuery eq(String name, Object value);

  /**
   * Add condition using primary key
   */
  ExtraQuery id(Object value);

  ExtraQuery select(String... field);

  ExtraQuery addField(String... cols);

  ExtraQuery setIndexName(String indexName);

  @Override
  String toString();
}
