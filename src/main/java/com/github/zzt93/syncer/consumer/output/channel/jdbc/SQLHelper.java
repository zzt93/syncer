package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

/**
 * @author zzt
 */
public class SQLHelper {

  private static final Logger logger = LoggerFactory.getLogger(SQLHelper.class);

  public static String inSQL(Object value) {
    if (value == null) {
      return "NULL";
    }
    if (ClassUtils.isPrimitiveOrWrapper(value.getClass())
        || CharSequence.class.isAssignableFrom(value.getClass())) {
      if (value instanceof String) {
        value = "'" + StringEscapeUtils.escapeSql(value.toString()) + "'";
      }
    } else {
      logger.error("Unhandled complex type: {}, value: {}", value.getClass(), value);
    }
    return value.toString();
  }

}
