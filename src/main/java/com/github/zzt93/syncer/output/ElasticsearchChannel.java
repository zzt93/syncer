package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncEvent;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import java.util.List;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements OutputChannel{

    private ElasticsearchTemplate esTemplate;

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
