package com.github.zzt93.syncer.config.consumer.input;

import com.github.zzt93.syncer.common.util.RegexUtil;
import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.consumer.Hashable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
@ConsumerConfig("input[].repos[]")
public class Repo implements Hashable {

  private final Logger logger = LoggerFactory.getLogger(Repo.class);

  @ConsumerConfig
  private String name;
  @ConsumerConfig
  private List<Entity> entities;

  private Pattern namePattern;
  private final HashMap<String, Fields> nameToRows = new HashMap<>();

  public Repo() {
  }

  public Repo(String name, List<Entity> entities) {
    this.name = name;
    setEntities(entities);
  }

  /**
   * because this config class is mutable, have to copy for re-use
   * @see #removeTableRow(String, String)
   */
  public Repo(Repo repo) {
    name = repo.name;
    namePattern= repo.namePattern;
    setEntities(repo.getEntities());
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
    namePattern = RegexUtil.getRegex(name);
  }

  public List<Entity> getEntities() {
    return entities;
  }

  public void setEntities(List<Entity> entities) {
    this.entities = entities;
    for (Entity entity : entities) {
      nameToRows.put(entity.getName(), entity.getField());
    }
    if (nameToRows.size() != entities.size()) {
      logger.warn("Duplicate entity name definition: {}", entities);
    }
  }

  /**
   * If {@link #name} is a regex pattern, connect to default database; <p></p> Otherwise, connect to
   * that specific database.
   *
   * @return the database name used in jdbc connection string
   */
  public String getConnectionName() {
    if (noNamePattern()) {
      return name;
    }
    return MysqlConnection.DEFAULT_DB;
  }

  public Pattern getNamePattern() {
    return namePattern;
  }

  public boolean noNamePattern() {
    return namePattern == null;
  }

  @Override
  public String toString() {
    return "Repo{" +
        "name='" + name + '\'' +
        ", entities=" + entities +
        ", namePattern=" + namePattern +
        '}';
  }

  public Fields getTableRow(String tableSchema, String tableName) {
    if (name.equals(tableSchema) ||
        (namePattern != null && namePattern.matcher(tableSchema).find())) {
      return nameToRows.getOrDefault((tableName), null);
    }
    return null;
  }

  public Fields removeTableRow(String tableSchema, String tableName) {
    if (name.equals(tableSchema) ||
        (namePattern != null && namePattern.matcher(tableSchema).find())) {
      return nameToRows.remove(tableName);
    }
    return null;
  }

  public boolean contain(String tableSchema, String tableName) {
    if (name.equals(tableSchema) ||
        (namePattern != null && namePattern.matcher(tableSchema).find())) {
      return nameToRows.containsKey(tableName);
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Repo repo = (Repo) o;

    return Objects.equals(name, repo.name);
  }

  @Override
  public int hashCode() {
    return name != null ? name.hashCode() : 0;
  }

  public Set<Entity> coldStart() {
    return getEntities().stream().filter(Entity::isCodeStart).collect(Collectors.toSet());
  }
}
