package com.github.zzt93.syncer.data;

/**
 * ----------- index/insert by query ------------
 *
 * @see SyncByQuery
 */
public interface ExtraQuery {

  ExtraQuery setTypeName(String typeName);

  ExtraQuery filter(String field, Object value);

  ExtraQuery select(String... field);

  ExtraQuery addField(String... cols);

  ExtraQuery setIndexName(String indexName);

  @Override
  String toString();
}
