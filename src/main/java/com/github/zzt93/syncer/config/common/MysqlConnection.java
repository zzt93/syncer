package com.github.zzt93.syncer.config.common;

/**
 * @author zzt
 */
public class MysqlConnection extends Connection {

    public String toConnectionUrl(String schemaName) {
        String url = "jdbc:mysql://" + getAddress() + ":" + getPort() + "/";

        return url + schemaName;
    }
}
