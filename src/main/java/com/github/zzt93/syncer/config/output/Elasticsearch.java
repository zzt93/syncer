package com.github.zzt93.syncer.config.output;

import com.github.zzt93.syncer.output.OutputChannel;

import java.util.List;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

    private String clusterName = "elasticsearch";
    private List<String> clusterNodes;

    private String user;
    private String passwordFile;
    private String index;
    private String type;
    private String documentId;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(List<String> clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswordFile() {
        return passwordFile;
    }

    public void setPasswordFile(String passwordFile) {
        this.passwordFile = passwordFile;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    @Override
    public void connect() {

    }

    @Override
    public OutputChannel build() {
        connect();
        return null;
    }
}
