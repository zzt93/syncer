package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import java.sql.Timestamp;
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
    Class<?> aClass = value.getClass();
    if (ClassUtils.isPrimitiveOrWrapper(aClass)
        || CharSequence.class.isAssignableFrom(aClass)
        || value instanceof Timestamp) {
      if (value instanceof String || value instanceof Timestamp) {
        value = "'" + StringEscapeUtils.escapeSql(value.toString()) + "'";
      }
    } else {
      logger.error("Unhandled complex type: {}, value: {}", aClass, value);
    }
    return value.toString();
  }

}
