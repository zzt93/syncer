package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author zzt
 */
@Component
@ConditionalOnProperty(prefix = "syncer.output.elasticsearch", name = {"cluster-name", "cluster-nodes[0]"})
public class ElasticsearchChannel implements OutputChannel {

    private ElasticsearchTemplate esTemplate;

    @Autowired
    public ElasticsearchChannel(ElasticsearchTemplate esTemplate) {
        this.esTemplate = esTemplate;
    }

    @Override
    public boolean output(SyncEvent event) {
        return false;
    }

    @Override
    public boolean output(List<SyncEvent> batch) {
        return false;
    }
}
