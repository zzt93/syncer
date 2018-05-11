package com.github.zzt93.syncer.consumer.output.channel.elastic;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import com.github.zzt93.syncer.common.data.SyncByQuery;
import com.github.zzt93.syncer.common.data.SyncByQueryES;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.config.pipeline.output.elastic.ESRequestMapping;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;
import com.google.common.base.Preconditions;
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
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class ESRequestMapper implements Mapper<SyncData, Object> {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final ESRequestMapping esRequestMapping;
  private final TransportClient client;
  private final KVMapper requestBodyMapper;
  private final Expression indexExpr;
  private final Expression typeExpr;
  private final Expression idExpr;

  public ESRequestMapper(TransportClient client, ESRequestMapping esRequestMapping) {
    this.esRequestMapping = esRequestMapping;
    this.client = client;
    SpelExpressionParser parser = new SpelExpressionParser();
    indexExpr = parser.parseExpression(esRequestMapping.getIndex());
    typeExpr = parser.parseExpression(esRequestMapping.getType());
    idExpr = parser.parseExpression(esRequestMapping.getDocumentId());

    ESQueryMapper esQueryMapper;
    if (esRequestMapping.getEnableExtraQuery()) {
      esQueryMapper = new ESQueryMapper(client);
    } else {
      esQueryMapper = null;
    }
    requestBodyMapper = new KVMapper(esRequestMapping.getFieldsMapping(), esQueryMapper);
  }

  @ThreadSafe(safe = {SpelExpressionParser.class, ESRequestMapping.class, TransportClient.class})
  @Override
  public Object map(SyncData data) {
    StandardEvaluationContext context = data.getContext();
    String index = eval(indexExpr, context);
    String type = eval(typeExpr, context);
    String id = eval(idExpr, context);
    switch (data.getType()) {
      case WRITE_ROWS:
        if (esRequestMapping.getNoUseIdForIndex()) {
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
              .script(getScript(data, data.getRecords()));
        }
        if (needScript(data)) {
          HashMap<String, Object> map = requestBodyMapper.map(data);
          return client.prepareUpdate(index, type, id)
              .setScript(getScript(data, map))
              .setRetryOnConflict(esRequestMapping.getRetryOnUpdateConflict());
        } else {
          return client.prepareUpdate(index, type, id).setDoc(requestBodyMapper.map(data))
              .setRetryOnConflict(esRequestMapping.getRetryOnUpdateConflict());
        }
    }
    throw new IllegalArgumentException("Unsupported row event type: " + data);
  }

  private boolean needScript(SyncData data) {
    SyncByQuery syncByQuery = data.syncByQuery();
    return syncByQuery!=null && ((SyncByQueryES) syncByQuery).needScript();
  }

  private String eval(Expression expr, StandardEvaluationContext context) {
    return expr.getValue(context, String.class);
  }

  private Script getScript(SyncData data, HashMap<String, Object> toSet) {
    HashMap<String, Object> params = new HashMap<>();
    StringBuilder code = new StringBuilder();
    makeScript(code, " = params.", ";", toSet, params);
    SyncByQuery syncByQuery = data.syncByQuery();
    if (syncByQuery instanceof SyncByQueryES) {
      // handle append/remove elements from list/array field
      makeScript(code, ".add(params.", ");", ((SyncByQueryES) syncByQuery).getAppend(), params);
      makeRemoveScript(code, ((SyncByQueryES) syncByQuery).getRemove(), params);
    } else {
      Preconditions.checkState(false, "should be `SyncByQueryES`");
    }
    return new Script(ScriptType.INLINE, "painless", code.toString(), params);
  }

  private void makeRemoveScript(StringBuilder code, HashMap<String, Object> remove,
      HashMap<String, Object> params) {
    for (String col : remove.keySet()) {
      code.append("ctx._source.").append(col).append(".remove(ctx._source.").append(col)
          .append(".lastIndexOf(params.").append(col).append("));");
    }
    scriptCheck(code, remove, params);
  }

  private void makeScript(StringBuilder code, String op, String endOp, HashMap<String, Object> data,
      HashMap<String, Object> params) {
    for (String col : data.keySet()) {
      code.append("ctx._source.").append(col).append(op).append(col).append(endOp);
    }
    scriptCheck(code, data, params);
  }

  private void scriptCheck(StringBuilder code, HashMap<String, Object> data,
      HashMap<String, Object> params) {
    int before = params.size();
    params.putAll(data);
    if (before + data.size() != params.size()) {
      throw new InvalidConfigException("Key conflict happens when making script [" + code + "], "
          + "check config file about `syncByQuery()` (Notice the `syncByQuery()` will default use all field for 'set' update)");
    }
  }

  private QueryBuilder getFilter(SyncData data) {
    BoolQueryBuilder builder = boolQuery();
    HashMap<String, Object> syncBy = data.getSyncBy();
    if (syncBy.isEmpty()) {
      throw new InvalidConfigException("No data used to do sync(update/delete) filter");
    }
    for (Entry<String, Object> entry : syncBy.entrySet()) {
      builder.filter(termQuery(entry.getKey(), entry.getValue()));
    }
    return builder;
  }
}
