package com.github.zzt93.syncer.producer.input.mysql.meta;

import com.github.zzt93.syncer.config.pipeline.common.MasterSource;
import com.github.zzt93.syncer.config.pipeline.input.Schema;
import com.github.zzt93.syncer.consumer.ConsumerSource;

import java.util.Objects;
import java.util.Set;

/**
 * All schemas a consumer requested for a remote DB.
 *
 * @see MasterSource#getSchemaSet()
 * @author zzt
 */
public class Consumer {

  private final Set<Schema> schemas;
  private final String id;

  public Consumer(ConsumerSource consumerSource) {
    this.schemas = consumerSource.getSchemas();
    id = consumerSource.clientId();
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Consumer consumer = (Consumer) o;
    return Objects.equals(id, consumer.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public Set<Schema> getSchemas() {
    return schemas;
  }

  @Override
  public String toString() {
    return "Consumer{" +
        "schemas=" + schemas +
        ", id='" + id + '\'' +
        '}';
  }
}
