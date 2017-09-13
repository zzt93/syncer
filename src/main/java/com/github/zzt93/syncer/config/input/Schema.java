package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.common.Table;
import com.github.zzt93.syncer.common.util.RegexUtil;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class Schema {

  private String name;
  private Set<Table> tables = new HashSet<>();

  private Pattern namePattern;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
    namePattern = RegexUtil.getRegex(name);
  }

  public Set<Table> getTables() {
    return tables;
  }

  public void setTables(Set<Table> tables) {
    this.tables = tables;
  }

  /**
   * If {@link #name} is a regex pattern, connect to default database;
   * <p></p>
   * Otherwise, connect to that database.
   *
   * @return the database name used in jdbc connection string
   */
  public String getConnectionName() {
    if (namePattern == null) {
      return name;
    }
    return "";
  }

  public Pattern getNamePattern() {
    return namePattern;
  }

  public boolean hasTable(String tableSchema, String tableName) {
    if (name.equals(tableSchema) ||
        (namePattern!=null && namePattern.matcher(tableSchema).find())) {
      return tables.contains(new Table(tableName));
    }
    return false ;
  }

  @Override
  public String toString() {
    return "Schema{" +
        "name='" + name + '\'' +
        '}';
  }
}
