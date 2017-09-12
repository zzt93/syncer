package com.github.zzt93.syncer.config.input;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author zzt
 */
public class Input {
    private Logger logger = LoggerFactory.getLogger(Input.class);

    private List<MysqlMaster> mysqlMasters = new ArrayList<>();
    private Set<MysqlMaster> mysqlMasterSet = new HashSet<>();

    public List<MysqlMaster> getMysqlMasters() {
        return mysqlMasters;
    }

    public void setMysqlMasters(List<MysqlMaster> mysqlMasters) {
        this.mysqlMasters = mysqlMasters;
        mysqlMasterSet.addAll(mysqlMasters);
        if (mysqlMasterSet.size() < mysqlMasters.size()) {
            logger.warn("Duplicate mysql master connection endpoint");
        }
    }

    public void connect() throws IOException {
        for (MysqlMaster mysqlMaster : mysqlMasterSet) {
            try {
                mysqlMaster.connect();
            } catch (IOException e) {
               logger.error("Fail to connect to mysql endpoint", mysqlMaster);
            }
        }
    }
}
