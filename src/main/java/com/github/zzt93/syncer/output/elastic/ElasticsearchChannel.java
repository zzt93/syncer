package com.github.zzt93.syncer.output.elastic;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.pipeline.output.DocumentMapping;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.output.BatchBuffer;
import com.github.zzt93.syncer.output.OutputChannel;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class ElasticsearchChannel implements OutputChannel {

  private final BatchBuffer<WriteRequestBuilder> batchBuffer;
  private final ESDocumentMapper esDocumentMapper;
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final TransportClient client;

  public ElasticsearchChannel(ElasticsearchConnection connection, DocumentMapping documentMapping,
      PipelineBatch batch)
      throws Exception {
    client = connection.transportClient();
    this.batchBuffer = new BatchBuffer<>(batch);
    this.esDocumentMapper = new ESDocumentMapper(documentMapping, client);
  }

  @Override
  public boolean output(SyncData event) {
    return batchBuffer.add(esDocumentMapper.map(event));
  }

  @Override
  public boolean output(List<SyncData> batch) {
    List<WriteRequestBuilder> collect = batch.stream().map(esDocumentMapper::map)
        .collect(Collectors.toList());
    return batchBuffer.addAll(collect);
  }

  @Override
  public BatchBuffer getBuffer() {
    return batchBuffer;
  }

  @Override
  public String des() {
    return "ElasticsearchChannel{" +
        "esTemplate=" + client +
        '}';
  }
}
