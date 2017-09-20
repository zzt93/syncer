package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.output.DocumentMapping;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements OutputChannel {

  private Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private ElasticsearchTemplate esTemplate;

  public ElasticsearchChannel(ElasticsearchConnection connection, DocumentMapping documentMapping)
      throws Exception {
    esTemplate = new ElasticsearchTemplate(connection.transportClient(),
        new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()));
  }

  @Override
  public boolean output(SyncData event) {

    logger.warn("Deleting rows: {}, can't handle", event);
    return false;
  }

  @Override
  public boolean output(List<SyncData> batch) {
    return false;
  }

  @Override
  public String des() {
    return "ElasticsearchChannel{" +
        "esTemplate=" + esTemplate +
        '}';
  }
}
