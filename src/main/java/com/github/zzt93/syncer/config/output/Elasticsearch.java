package com.github.zzt93.syncer.config.output;

import com.github.zzt93.syncer.config.share.ElasticsearchConnection;
import org.springframework.context.annotation.Configuration;

/**
 * @author zzt
 */
public class Elasticsearch implements OutputChannelConfig {

    private ElasticsearchConnection connection;
    private InputMapping inputMapping;

    public ElasticsearchConnection getConnection() {
        return connection;
    }

    public void setConnection(ElasticsearchConnection connection) {
        this.connection = connection;
    }

    public InputMapping getInputMapping() {
        return inputMapping;
    }

    public void setInputMapping(InputMapping inputMapping) {
        this.inputMapping = inputMapping;
    }

}
