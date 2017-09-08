package com.github.zzt93.syncer.common;

import java.util.HashMap;

/**
 * @author zzt
 */
public class SyncData {

    private Table table;
    private HashMap<String, Object> rows;

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }
}
