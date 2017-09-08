package com.github.zzt93.syncer.config.input;

import com.github.zzt93.syncer.config.share.Connection;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Input {

    private Mysql mysql;
    private List<Connection> connections = new ArrayList<>();

    public List<Connection> getConnections() {
        return connections;
    }

    public void setConnections(List<Connection> connections) {
        this.connections = connections;
    }

}
