package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.producer.input.mysql.AlterMeta;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represent metadata for a group of related database/schema
 *
 * @author zzt
 */
@Slf4j
public class SchemaMeta {

  /**
   * can't use {@link java.util.HashMap}, because it is not immutable
   */
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

  public String getSchema() {
    return schema;
  }

  public int size() {
    return tableMetas.size();
  }

  @Override
  public String toString() {
    return "SchemaMeta{" +
        "schema='" + schema + '\'' +
        ", schemaPattern=" + schemaPattern +
        ", tableMetas(" + tableMetas.size() + ")=" + tableMetas +
        '}';
  }

  boolean updateTableMeta(AlterMeta alterMeta, TableMeta full) {
    TableMeta table = findTable(alterMeta.getSchema(), alterMeta.getTable());
    if (table != null) {
      log.info("Updating table meta for {} with {}", table, full);
      if (table.isAll()) {
        tableMetas.put(alterMeta.getTable(), full);
      } else {
        table.update(full);
      }
      return true;
    }
    return false;
  }

  public List<TableMeta> codeStart() {
    return tableMetas.values().stream().filter(TableMeta::isCodeStart).collect(Collectors.toList());
  }

}
