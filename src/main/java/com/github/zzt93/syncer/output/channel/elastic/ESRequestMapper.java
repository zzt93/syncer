package com.github.zzt93.syncer.output.channel.elastic;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.output.RequestMapping;
import com.github.zzt93.syncer.output.mapper.JsonMapper;
import com.github.zzt93.syncer.output.mapper.Mapper;
import java.util.Collections;
import java.util.HashMap;
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
  private final JsonMapper requestBodyMapper;

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
    requestBodyMapper = new JsonMapper(requestMapping.getFieldsMapping(), esQueryMapper);
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
        return client.prepareUpdate(index, type, id).setDoc(requestBodyMapper.map(data));
    }
    throw new IllegalArgumentException("Invalid row event type");
  }

  private String eval(String expr, StandardEvaluationContext context) {
   return parser
        .parseExpression(expr)
        .getValue(context, String.class);
  }

  private Script getScript(SyncData data) {
    HashMap<String, Object> update = data.getRow();
    StringBuilder code = new StringBuilder();
    StandardEvaluationContext context = data.getContext();
    for (String col : update.keySet()) {
      String expr = update.get(col).toString();
      code.append("ctx._source.").append(col).append(" = ").append(eval(expr, context)).append(";");
    }
    return new Script(ScriptType.INLINE, "painless", code.toString(), Collections.emptyMap());
  }

  private QueryBuilder getFilter(SyncData data) {
    BoolQueryBuilder builder = boolQuery();
    HashMap<String, Object> syncBy = data.getSyncBy();
    StandardEvaluationContext context = data.getContext();
    for (String s : syncBy.keySet()) {
      String expr = syncBy.get(s).toString();
      builder.filter(termQuery(s, eval(expr, context)));
    }
    return builder;
  }
}
