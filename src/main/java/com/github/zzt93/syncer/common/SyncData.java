package com.github.zzt93.syncer.common;

import java.util.HashMap;

/**
 * @author zzt
 */
public class SyncData {

    private final Table table;
    private final HashMap<String, Object> rows = new HashMap<>();

    public SyncData(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public SyncData addPair(String colName, Object value) {
        rows.put(colName, value);
        return this;
    }

    public HashMap<String, Object> getRows() {
        return rows;
    }
}
