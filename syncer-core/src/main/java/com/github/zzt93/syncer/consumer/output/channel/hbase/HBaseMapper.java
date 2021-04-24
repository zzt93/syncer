package com.github.zzt93.syncer.consumer.output.channel.hbase;

import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseMapper implements Mapper<SyncData, Mutation> {

  public static final String CF = "cf";

  @Override
  public Mutation map(SyncData syncData) {
    switch (syncData.getType()) {
      case WRITE:
      case UPDATE:
        return put(syncData);
      case DELETE:
        return deleteRow(syncData.getDbId());
    }
    return null;
  }

  private Put put(SyncData syncData) {
    HashMap<String, Object> fields = syncData.getFields();
    Put put = new Put(toBytes(syncData.getDbId()));
    for (Map.Entry<String, Object> e : fields.entrySet()) {
      if (e.getValue() == null) continue;
      String key = e.getKey();
      put.addColumn(toBytes(syncData.getColumnFamily(key)), toBytes(key), typeConversion(e.getValue()));
    }
    return put;
  }

  private byte[] typeConversion(Object s) {
    if (s instanceof String) {
      return toBytes(((String) s));
    } else if (s instanceof Boolean) {
      return toBytes(((Boolean) s));
    } else if (s instanceof Long) {
      return toBytes(((Long) s));
    } else if (s instanceof Float) {
      return toBytes(((Float) s));
    } else if (s instanceof Double) {
      return toBytes(((Double) s));
    } else if (s instanceof Integer) {
      return toBytes(((Integer) s));
    } else if (s instanceof Short) {
      return toBytes(((Short) s));
    } else if (s instanceof BigDecimal) {
      return toBytes(((BigDecimal) s));
    } else if (s instanceof byte[]) {
      return ((byte[]) s);
    } else if (s instanceof Timestamp) {
      return toBytes(((Timestamp) s).getTime());
    } else if (s instanceof Date) {
      return toBytes(((Date) s).getTime());
    }
    throw new IllegalArgumentException("Unsupported type: " + s.getClass());
  }

  private static Delete deleteColumnFamily(String rowKey, String columnFamily) {
    Delete delete = new Delete(toBytes(rowKey));
    delete.addFamily(toBytes(columnFamily));
    return delete;
  }

  private static Delete deleteColumn(String rowKey, String columnFamily, String columnName) {
    Delete delete = new Delete(toBytes(rowKey));
    delete.addColumn(toBytes(columnFamily), toBytes(columnName));
    return delete;
  }

  private Delete deleteRow(String rowKey) {
    return new Delete(toBytes(rowKey));
  }

}
