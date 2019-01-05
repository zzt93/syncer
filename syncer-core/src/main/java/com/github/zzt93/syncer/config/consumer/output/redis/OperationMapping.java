package com.github.zzt93.syncer.config.consumer.output.redis;

import com.github.zzt93.syncer.config.consumer.output.redis.ValueType.Operation;
import org.springframework.data.redis.connection.RedisConnection;

import java.util.function.BiFunction;

/**
 * @author zzt
 */
public class OperationMapping {

  private ValueType valueType = ValueType.STRING;
  private Operation operation;
  private String key;
  private String value;
  // TODO 18/2/12 may add key/value serializer

  public ValueType getValueType() {
    return valueType;
  }

  public void setValueType(
      ValueType valueType) {
    this.valueType = valueType;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public BiFunction<byte[], byte[], Object> operationMethod(
      RedisConnection connection) {
    return valueType.biFunction(connection, operation);
  }
}
