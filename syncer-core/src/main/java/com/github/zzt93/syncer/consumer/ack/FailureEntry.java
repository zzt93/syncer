package com.github.zzt93.syncer.consumer.ack;

import java.time.LocalDateTime;

/**
 * @author zzt
 */
public class FailureEntry <T> {

  private final T data;
  private final String timestamp;
  private final String exception;

  FailureEntry(T data, LocalDateTime timestamp, String exception) {
    this.data = data;
    this.timestamp = timestamp.toString();
    this.exception = exception;
  }
}
