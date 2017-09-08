package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.common.Table;

import java.util.List;

/**
 * @author zzt
 */
public class Schema {

    private String name;
    private List<Table> tables;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }
}
