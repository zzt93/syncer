package com.github.zzt93.syncer.config.consumer.input;

import com.github.zzt93.syncer.config.ConsumerConfig;
import lombok.Data;

import java.util.List;

/**
 * @author zzt
 */
@Data
@ConsumerConfig("input.masters[].repos[].entities[]")
public class Entity {

  private String name;
  private List<String> fields;

  public Entity() {
  }

  public Entity(String tableName) {
    this.name = tableName;
  }

  public Fields getFields() {
    return new Fields(fields);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Entity entity = (Entity) o;

    return name.equals(entity.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

}
