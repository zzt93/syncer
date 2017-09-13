package com.github.zzt93.syncer.common;

import java.util.HashMap;

/**
 * @author zzt
 */
public class SyncData {

    private final String schema;
    private final String table;
    private final HashMap<String, Object> row = new HashMap<>();
    private final HashMap<String, Object> extra = new HashMap<>();

    public SyncData(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public SyncData(MysqlRowEvent rowEvent) {
        schema = "";
        table = "";
    }

    public String getTable() {
        return table;
    }

    public SyncData addRow(String colName, Object value) {
        row.put(colName, value);
        return this;
    }

    public SyncData addExtra(String colName, Object value) {
        extra.put(colName, value);
        return this;
    }

    public HashMap<String, Object> getRow() {
        return row;
    }

    public HashMap<String, Object> getExtra() {
        return extra;
    }

    public String getSchema() {
        return schema;
    }
}
