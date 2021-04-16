package com.github.zzt93.syncer.config.consumer.input;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.ColdStartConfig;
import lombok.Data;

import java.util.List;

/**
 * @author zzt
 */
@Data
@ConsumerConfig("input.masters[].repos[].entities[]")
public class Entity {

  @ConsumerConfig
  private String name;
  @ConsumerConfig
  private List<String> fields;
  @ConsumerConfig
  private ColdStartConfig cold;

  private Fields field = new Fields(null);

  public Entity() {
  }

  public Entity(String tableName) {
    this.name = tableName;
  }

  public void setFields(List<String> fields) {
    this.fields = fields;
    field = new Fields(fields);
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

  public boolean isCodeStart() {
    return cold != null;
  }

  public boolean containField(String key) {
    return field.contains(key);
  }
}
