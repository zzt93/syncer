package com.github.zzt93.syncer.data;

import java.util.HashMap;
import java.util.Map;

/**
 * ----------- index/insert by query ------------
 *
 * @see SyncByQuery
 */
public interface ExtraQuery {

  String getTypeName();

  ExtraQuery setTypeName(String typeName);

  ExtraQuery filter(String field, Object value);

  ExtraQuery select(String... field);

  ExtraQuery addField(String... cols);

  String getIndexName();

  ExtraQuery setIndexName(String indexName);

  HashMap<String, Object> getQueryBy();

  String[] getSelect();

  String getAs(int i);

  void addQueryResult(Map<String, Object> result);

  Object getQueryResult(String key);

  Object getField(String s);

  @Override
  String toString();
}
