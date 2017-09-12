package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.input.connect.MasterConnector;

import java.io.IOException;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MysqlMaster that = (MysqlMaster) o;

        return connection.equals(that.connection);
    }

    @Override
    public int hashCode() {
        return connection.hashCode();
    }

    void connect() throws IOException {
        new MasterConnector(connection).connect();
    }
}
