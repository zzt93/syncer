package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.data.util.SQLFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;

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
        || value instanceof Timestamp
        || value instanceof BigDecimal) {
      if (value instanceof String) {
        // TODO 2019/3/3 http://www.jguru.com/faq/view.jsp?EID=8881 {escape '/'} ?
        String replace = StringUtils.replace(StringUtils.replace(value.toString(), "'", "''"), "\\", "\\\\");
        value = "'" + replace + "'";
      } else if (value instanceof Timestamp) {
        value = "'" + value.toString() + "'";
      }
    } else if (SQLFunction.class.isAssignableFrom(aClass)) {
      value = value.toString();
    } else {
      logger.error("Unhandled complex type: {}, value: {}", aClass, value);
    }
    return value.toString();
  }

  public static String wrapCol(String col) {
    return '`' + col + '`';
  }

}
