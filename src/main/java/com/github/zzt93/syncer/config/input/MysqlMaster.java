package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.config.share.Connection;

/**
 * @author zzt
 */
public class MysqlMaster {

    private Connection connection;
    private Schema schema;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
