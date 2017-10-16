package com.github.zzt93.syncer.output.channel.elastic;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.output.DocumentMapping;
import com.github.zzt93.syncer.output.mapper.JsonMapper;
import com.github.zzt93.syncer.output.mapper.Mapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class ESDocumentMapper implements Mapper<SyncData, Object> {

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

  @ThreadSafe(safe = {SpelExpressionParser.class, DocumentMapping.class, TransportClient.class})
  @Override
  public Object map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
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
        if (documentMapping.getNoUseIdForIndex()) {
          return client.prepareIndex(index, type).setSource(jsonMapper.map(data));
        }
        return client.prepareIndex(index, type, id).setSource(jsonMapper.map(data));
      case DELETE_ROWS:
        logger.info("Deleting data from Elasticsearch, may affect performance");
        if (data.isSyncWithoutId()) {
          logger.warn("Deleting data by query, may affect performance");
          return DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
              .filter(matchQuery(data.getSyncWithCol(), data.getRowValue(data.getSyncWithCol())))
              .source(index);
        }
        return client.prepareDelete(index, type, id);
      case UPDATE_ROWS:
        if (data.isSyncWithoutId()) {
          return UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
              .filter(matchQuery(data.getSyncWithCol(), data.getRowValue(data.getSyncWithCol())))
              .source(index);
        }
        return client.prepareUpdate(index, type, id).setDoc(jsonMapper.map(data));
    }
    throw new IllegalArgumentException("Invalid row event type");
  }
}
