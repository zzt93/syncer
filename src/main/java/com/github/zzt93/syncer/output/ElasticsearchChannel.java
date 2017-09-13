package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Component;

/**
 * @author zzt
 */
@Component
@ConditionalOnProperty(prefix = "syncer.output.elasticsearch.connection", name = {"cluster-name", "cluster-nodes[0]"})
public class ElasticsearchChannel implements OutputChannel {
    private Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
    private ElasticsearchTemplate esTemplate;

    @Autowired
    public ElasticsearchChannel(ElasticsearchTemplate esTemplate) {
        this.esTemplate = esTemplate;
    }

    @Override
    public boolean output(SyncData event) {
        logger.warn("Deleting rows, should happen", event);
        return false;
    }

    @Override
    public boolean output(List<SyncData> batch) {
        return false;
    }
}
