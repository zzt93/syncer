package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.common.thread.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Represent metadata for a group of related database/schema
 *
 * @author zzt
 */
public class SchemaMeta {

  /**
   * can't use {@link java.util.HashMap}, because it is not immutable
   */
  @ThreadSafe(des = "effectively immutable & published by 'Thread start rule'. change to HashMap?",
      sharedBy = "remote mysql connection, main")
  private final ConcurrentHashMap<String, TableMeta> tableMetas = new ConcurrentHashMap<>();
  private final Pattern schemaPattern;
  private final String schema;

  SchemaMeta(String name, Pattern namePattern) {
    this.schemaPattern = namePattern;
    this.schema = name;
  }

  TableMeta addTableMeta(String name, TableMeta tableMeta) {
    return tableMetas.put(name, tableMeta);
  }

  @ThreadSafe(safe = {Pattern.class, ConcurrentHashMap.class})
  public TableMeta findTable(String database, String table) {
    // schema match?
    if (schema.equals(database) ||
        (schemaPattern != null && schemaPattern.matcher(database).matches())) {
      // table name match?
      return tableMetas.getOrDefault(table, null);
    }
    return null;
  }

  public int size() {
    return tableMetas.size();
  }

  @Override
  public String toString() {
    return "SchemaMeta{" +
        "tableMetas=" + tableMetas +
        ", schemaPattern=" + schemaPattern +
        ", schema='" + schema + '\'' +
        '}';
  }
}
