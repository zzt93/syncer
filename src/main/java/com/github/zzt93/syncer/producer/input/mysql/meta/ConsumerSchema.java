package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.config.pipeline.input.Schema;
import java.util.Set;

/**
 * @author zzt
 */
public class ConsumerSchema {

  private final Set<Schema> schemas;

  public ConsumerSchema(Set<Schema> schemas) {
    this.schemas = schemas;
  }

  public Set<Schema> getSchemas() {
    return schemas;
  }
}
