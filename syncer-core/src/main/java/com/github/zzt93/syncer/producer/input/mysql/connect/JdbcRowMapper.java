package com.github.zzt93.syncer.producer.input.mysql.connect;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zzt
 */
class JdbcRowMapper implements RowMapper<Map<String, Object>> {
  private static final BigInteger LONG_MAX = new BigInteger("" + Long.MAX_VALUE);
  @Override
  public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
    ResultSetMetaData rsMeta = rs.getMetaData();
    int columnCount = rsMeta.getColumnCount();
    Map<String, Object> mapOfColValues = new HashMap<>(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      String key = JdbcUtils.lookupColumnName(rsMeta, i);
      Object obj = JdbcUtils.getResultSetValue(rs, i);
      if (obj instanceof BigInteger && LONG_MAX.compareTo((BigInteger) obj) >= 0) {
        obj = ((BigInteger) obj).longValue();
      }
      mapOfColValues.put(key, obj);
    }
    return mapOfColValues;
  }
}
