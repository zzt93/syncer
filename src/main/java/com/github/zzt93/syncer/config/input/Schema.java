package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.common.Table;
import com.github.zzt93.syncer.util.RegexUtil;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author zzt
 */
public class Schema {

    private String name;
    private Pattern namePattern;
    private List<Table> tables;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        namePattern = RegexUtil.getRegex(name);
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    /**
     * Connect to
     * @return
     */
    public String getConnectionName () {
        if (namePattern == null) {
            return name;
        }
        return "";
    }
}
