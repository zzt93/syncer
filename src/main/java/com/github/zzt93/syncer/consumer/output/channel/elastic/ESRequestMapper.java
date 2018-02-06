package com.github.zzt93.syncer.consumer.output.channel.elastic;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.output.RequestMapping;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;
import java.util.HashMap;
import java.util.Map.Entry;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class ESRequestMapper implements Mapper<SyncData, Object> {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final RequestMapping requestMapping;
  private final TransportClient client;
  private final SpelExpressionParser parser;
  private final KVMapper requestBodyMapper;

  public ESRequestMapper(TransportClient client, RequestMapping requestMapping) {
    this.requestMapping = requestMapping;
    this.client = client;
    parser = new SpelExpressionParser();
    ESQueryMapper esQueryMapper;
    if (requestMapping.getEnableExtraQuery()) {
      esQueryMapper = new ESQueryMapper(client);
    } else {
      esQueryMapper = null;
    }
    requestBodyMapper = new KVMapper(requestMapping.getFieldsMapping(), esQueryMapper);
  }

  @ThreadSafe(safe = {SpelExpressionParser.class, RequestMapping.class, TransportClient.class})
  @Override
  public Object map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    String index = eval(requestMapping.getIndex(), context);
    String type = eval(requestMapping.getType(), context);
    String id = eval(requestMapping.getDocumentId(), context);
    switch (data.getType()) {
      case WRITE_ROWS:
        if (requestMapping.getNoUseIdForIndex()) {
          return client.prepareIndex(index, type).setSource(requestBodyMapper.map(data));
        }
        return client.prepareIndex(index, type, id).setSource(requestBodyMapper.map(data));
      case DELETE_ROWS:
        logger.info("Deleting data from Elasticsearch, may affect performance");
        if (data.isSyncWithoutId()) {
          logger.warn("Deleting data by query, may affect performance");
          return DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
              .source(index)
              .filter(getFilter(data));
        }
        return client.prepareDelete(index, type, id);
      case UPDATE_ROWS:
        if (data.isSyncWithoutId()) {
          logger.warn("Updating data by query, may affect performance");
          return UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
              .source(index)
              .filter(getFilter(data))
              .script(getScript(data));
        }
        return client.prepareUpdate(index, type, id).setDoc(requestBodyMapper.map(data))
            .setRetryOnConflict(requestMapping.getRetryOnUpdateConflict());
    }
    throw new IllegalArgumentException("Unsupported row event type: " + data);
  }

  private String eval(String expr, StandardEvaluationContext context) {
    return parser
        .parseExpression(expr)
        .getValue(context, String.class);
  }

  private Script getScript(SyncData data) {
    HashMap<String, Object> update = data.getRecords();
    StringBuilder code = new StringBuilder();
    for (String col : update.keySet()) {
      code.append("ctx._source.").append(col).append(" = ").append(col).append(";");
    }
    return new Script(ScriptType.INLINE, "painless", code.toString(), update);
  }

  private QueryBuilder getFilter(SyncData data) {
    BoolQueryBuilder builder = boolQuery();
    HashMap<String, Object> syncBy = data.getSyncBy();
    for (Entry<String, Object> entry : syncBy.entrySet()) {
      builder.filter(termQuery(entry.getKey(), entry.getValue()));
    }
    return builder;
  }
}
