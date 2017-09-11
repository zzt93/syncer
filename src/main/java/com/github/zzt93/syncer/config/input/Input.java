package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.config.share.Connection;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Input {

    private List<MysqlMaster> mysqlMasters = new ArrayList<>();

    public List<MysqlMaster> getMysqlMasters() {
        return mysqlMasters;
    }

    public void setMysqlMasters(List<MysqlMaster> mysqlMasters) {
        this.mysqlMasters = mysqlMasters;
    }
}
