package com.github.zzt93.syncer.data;

/**
 * Write/Update/Delete by query <b>output channel</b>
 *
 * @see SyncByQuery
 */
public interface ExtraQuery {

  ExtraQuery setTypeName(String typeName);
  ExtraQuery setIndexName(String indexName);

  /**
   * Add condition for extraQuery
   * @see #eq(String, Object) for replace
   * @param name name of target field in repo.entity in <b>output channel</b>
   * @param value value of target field in repo.entity in <b>output channel</b>
   */
  @Deprecated
  ExtraQuery filter(String name, Object value);

  ExtraQuery eq(String name, Object value);

  /**
   * Add condition using primary key
   */
  ExtraQuery id(Object value);

  ExtraQuery select(String... field);

  /**
   * rename {@link #select(String...)} fields as new name
   * @param cols new name
   */
  ExtraQuery as(String... cols);

  /**
   * @see #as(String...) for replace
   */
  @Deprecated
  ExtraQuery addField(String... cols);

  @Override
  String toString();
}
