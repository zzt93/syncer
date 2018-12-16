package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.ESScriptUpdate;
import com.github.zzt93.syncer.common.data.SyncByQuery;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.InvalidSyncDataException;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.elastic.ESRequestMapping;
import com.github.zzt93.syncer.consumer.output.mapper.KVMapper;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;
import com.google.common.collect.Lists;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * @author zzt
 */
public class ESRequestMapper implements Mapper<SyncData, Object> {

  public static final ArrayList<Object> NEW = new ArrayList<>();
  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final ESRequestMapping esRequestMapping;
  private final AbstractClient client;
  private final KVMapper requestBodyMapper;
  private final Expression indexExpr;
  private final Expression typeExpr;
  private final Expression idExpr;

  public ESRequestMapper(AbstractClient client, ESRequestMapping esRequestMapping) {
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
        logger.info("Deleting doc from Elasticsearch, may affect performance");
        if (id != null) {
          return client.prepareDelete(index, type, id);
        }
        logger.warn("Deleting doc by query, may affect performance");
        return DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
            .source(index)
            .filter(getFilter(data));
      case UPDATE_ROWS:
        // Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
        if (id != null) { // update doc with `id`
          if (needScript(data)) { // scripted updates: update using script
            HashMap<String, Object> map = requestBodyMapper.map(data);
            UpdateRequestBuilder builder = client.prepareUpdate(index, type, id)
                .setScript(getScript(data, map))
                .setRetryOnConflict(esRequestMapping.getRetryOnUpdateConflict());
            if (esRequestMapping.isUpsert()) { // scripted_upsert
              builder.setUpsert(getUpsert(data)).setScriptedUpsert(true);
            }
            return builder;
          } else { // update with partial doc
            return client.prepareUpdate(index, type, id).setDoc(requestBodyMapper.map(data))
                .setDocAsUpsert(esRequestMapping.isUpsert()) // doc_as_upsert
                .setRetryOnConflict(esRequestMapping.getRetryOnUpdateConflict());
          }
        } else { // update doc by `query`
          logger.warn("Updating doc by query, may affect performance");
          return UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
              .source(index)
              .filter(getFilter(data))
              .script(getScript(data, data.getFields()));
        }
      default:
        throw new IllegalArgumentException("Unsupported row event type: " + data);
    }
  }

  private static boolean needScript(SyncData data) {
    SyncByQuery syncByQuery = data.syncByQuery();
    return syncByQuery != null && ((ESScriptUpdate) syncByQuery).needScript();
  }

  private String eval(Expression expr, StandardEvaluationContext context) {
    return expr.getValue(context, String.class);
  }

  private Script getScript(SyncData data, HashMap<String, Object> toSet) {
    HashMap<String, Object> params = new HashMap<>();
    StringBuilder code = new StringBuilder();
    makeScript(code, " = params.", ";", toSet, params);
    SyncByQuery syncByQuery = data.syncByQuery();
    if (syncByQuery instanceof ESScriptUpdate) {
      // handle append/remove elements from list/array field
      makeScript(code, ".add(params.", ");", ((ESScriptUpdate) syncByQuery).getAppend(), params);
      makeRemoveScript(code, ((ESScriptUpdate) syncByQuery).getRemove(), params);
    } else {
      throw new InvalidSyncDataException("[syncByQuery] should be [SyncByQueryES]", data);
    }
    return new Script(ScriptType.INLINE, "painless", code.toString(), params);
  }

  private void makeRemoveScript(StringBuilder code, HashMap<String, Object> remove,
      HashMap<String, Object> params) {
    for (String col : remove.keySet()) {
      code.append("ctx._source.").append(col).append(".remove(ctx._source.").append(col)
          .append(".indexOf(params.").append(col).append("));");
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
          + "check config file about `syncByQuery()` (Notice the `syncByQuery()` will default use all fields for 'set' update)");
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

  static Map getUpsert(SyncData data) {
    assert needScript(data);
    HashMap<String, Object> upsert = new HashMap<>();
    ESScriptUpdate syncByQuery = (ESScriptUpdate) data.syncByQuery();
    for (String col : syncByQuery.getAppend().keySet()) {
      upsert.put(col, NEW);
    }
    for (Entry<String, Object> entry : syncByQuery.getRemove().entrySet()) {
      upsert.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
    }
    return upsert;
  }
}
