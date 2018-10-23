package com.github.zzt93.syncer.common.exception;

import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;

/**
 * @author zzt
 */
public class FailureException extends RuntimeException {

  public FailureException(String message) {
    super(message);
  }

  public static String getErr(OutputChannel outputChannel, String id) {
    return "Failure log with too many failed items, aborting " + outputChannel.id() + "@" + id;
  }
}
