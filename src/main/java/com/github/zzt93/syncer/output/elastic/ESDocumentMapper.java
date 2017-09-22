package com.github.zzt93.syncer.output.elastic;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.output.DocumentMapping;
import com.github.zzt93.syncer.output.JsonMapper;
import com.github.zzt93.syncer.output.Mapper;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class ESDocumentMapper implements Mapper<SyncData, WriteRequestBuilder> {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final DocumentMapping documentMapping;
  private final TransportClient client;
  private final SpelExpressionParser parser;
  private final JsonMapper jsonMapper;

  public ESDocumentMapper(DocumentMapping documentMapping, TransportClient client) {
    this.documentMapping = documentMapping;
    this.client = client;
    parser = new SpelExpressionParser();
    jsonMapper = new JsonMapper(documentMapping.getFieldsMapper());
  }

  @Override
  public WriteRequestBuilder map(SyncData data) {
    StandardEvaluationContext context = new StandardEvaluationContext(data);
    String index = parser
        .parseExpression(documentMapping.getIndex(), ParserContext.TEMPLATE_EXPRESSION)
        .getValue(context, String.class);
    String type = parser
        .parseExpression(documentMapping.getType(), ParserContext.TEMPLATE_EXPRESSION)
        .getValue(context, String.class);
    String id = parser
        .parseExpression(documentMapping.getDocumentId(), ParserContext.TEMPLATE_EXPRESSION)
        .getValue(context, String.class);
    switch (data.getType()) {
      case WRITE_ROWS:
        return client.prepareIndex(index, type, id).setSource(jsonMapper.map(data));
      case DELETE_ROWS:
        logger.info("Deleting data from Elasticsearch, may affect performance");
        break;
      case UPDATE_ROWS:
        break;
    }
    throw new IllegalArgumentException("Invalid row event type");
  }
}
