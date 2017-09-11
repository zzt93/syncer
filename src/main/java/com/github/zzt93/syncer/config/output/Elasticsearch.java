package com.github.zzt93.syncer.config.output;

import com.github.zzt93.syncer.config.share.ElasticsearchConnection;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

    private ElasticsearchConnection connection;
    private IndexMapping indexMapping;

    public ElasticsearchConnection getConnection() {
        return connection;
    }

    public void setConnection(ElasticsearchConnection connection) {
        this.connection = connection;
    }

    public IndexMapping getIndexMapping() {
        return indexMapping;
    }

    public void setIndexMapping(IndexMapping indexMapping) {
        this.indexMapping = indexMapping;
    }

}
