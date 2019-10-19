package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.ESScriptUpdate;
import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.elastic.ESRequestMapping;
import com.github.zzt93.syncer.consumer.output.channel.mapper.KVMapper;
import com.google.common.collect.Lists;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * @author zzt
 *
 * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/painless-api-reference.html
 * https://www.elastic.co/guide/en/elasticsearch/painless/7.5/painless-api-reference-shared-java-util.html#painless-api-reference-shared-ArrayList
 */
public class ESRequestMapper implements Mapper<SyncData, Object> {

  private final Logger logger = LoggerFactory.getLogger(ElasticsearchChannel.class);
  private final ESRequestMapping esRequestMapping;
  private final KVMapper requestBodyMapper;
  private final Expression indexExpr;
  private final Expression typeExpr;
  private final ESQueryMapper esQueryMapper;

  public ESRequestMapper(RestHighLevelClient client, ESRequestMapping esRequestMapping) {
    this.esRequestMapping = esRequestMapping;
    SpelExpressionParser parser = new SpelExpressionParser();
    indexExpr = parser.parseExpression(esRequestMapping.getIndex());
    typeExpr = parser.parseExpression(esRequestMapping.getType());

    esQueryMapper = new ESQueryMapper(client);
    requestBodyMapper = new KVMapper(esRequestMapping.getFieldsMapping());
  }

  @ThreadSafe(safe = {SpelExpressionParser.class, ESRequestMapping.class, RestHighLevelClient.class})
  @Override
  public Object map(SyncData data) {
    esQueryMapper.parseExtraQueryContext(data.getExtraQueryContext());

    StandardEvaluationContext context = data.getContext();
    String index = eval(indexExpr, context);
    String type = eval(typeExpr, context);
    String id = data.getId() == null ? null : data.getId().toString();
    switch (data.getType()) {
      case WRITE:
        if (esRequestMapping.getNoUseIdForIndex()) {
          return new IndexRequest(index, type).source(requestBodyMapper.map(data));
        }
        return new IndexRequest(index, type, id).source(requestBodyMapper.map(data));
      case DELETE:
        logger.info("Deleting doc from Elasticsearch, may affect performance");
        if (id != null) {
          return new DeleteRequest(index, type, id);
        }
        logger.warn("Deleting doc by query, may affect performance");
        return new DeleteByQueryRequest(index).setQuery(getFilter(data));
      case UPDATE:
        // Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
        if (id != null) { // update doc with `id`
          if (needScript(data)) { // scripted updates: update using script
            HashMap<String, Object> map = requestBodyMapper.map(data);
            UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                .script(getScript(data, map))
                .retryOnConflict(esRequestMapping.getRetryOnUpdateConflict());
            if (esRequestMapping.isUpsert()) { // scripted_upsert
              updateRequest.upsert(getUpsert(data)).scriptedUpsert(true);
            }
            return updateRequest;
          } else { // update with partial doc
            return new UpdateRequest(index, type, id).doc(requestBodyMapper.map(data))
                .docAsUpsert(esRequestMapping.isUpsert()) // doc_as_upsert
                .retryOnConflict(esRequestMapping.getRetryOnUpdateConflict());
          }
        } else { // update doc by `query`
          logger.warn("Updating doc by query, may affect performance");
          return new UpdateByQueryRequest(index).setQuery(getFilter(data)).setScript(getScript(data, data.getFields()));
        }
      default:
        throw new IllegalArgumentException("Unsupported row event type: " + data);
    }
  }

  private static boolean needScript(SyncData data) {
    ESScriptUpdate esScriptUpdate = data.getEsScriptUpdate();
    return esScriptUpdate != null && esScriptUpdate.needScript();
  }

  private String eval(Expression expr, StandardEvaluationContext context) {
    return expr.getValue(context, String.class);
  }

  /**
   * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/painless-api-reference.html
   */
  private Script getScript(SyncData data, HashMap<String, Object> toSet) {
    HashMap<String, Object> params = new HashMap<>();
    StringBuilder code = new StringBuilder();
    ESScriptUpdate.makeScript(code, " = params.", ";", toSet, params);

    if (needScript(data)) {
      ESScriptUpdate esScriptUpdate = data.getEsScriptUpdate();
      esScriptUpdate.generateMergeScript(code, params);
    }

    return new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, code.toString(), params);
  }

  private QueryBuilder getFilter(SyncData data) {
    BoolQueryBuilder builder = boolQuery();
    HashMap<String, Object> syncBy = data.getSyncBy();
    if (syncBy.isEmpty()) {
      throw new InvalidConfigException("No data used to do sync(update/delete) filter");
    }
    for (Entry<String, Object> entry : syncBy.entrySet()) {
      String[] key = entry.getKey().split("\\.");
      if (key.length == 2) {
        builder.filter(nestedQuery(key[0], boolQuery().filter(getSingleFilter(entry)), ScoreMode.Avg));
      } else if (key.length == 1) {
        builder.filter(getSingleFilter(entry));
      } else {
        logger.error("Only support one level nested obj for the time being");
      }
    }
    return builder;
  }

  private QueryBuilder getSingleFilter(Entry<String, Object> entry) {
    Object value = entry.getValue();
    if (value instanceof Collection || value.getClass().isArray()) {
      return termsQuery(entry.getKey(), value);
    }
    return termQuery(entry.getKey(), value);
  }

  static Map getUpsert(SyncData data) {
    assert needScript(data);
    HashMap<String, Object> upsert = new HashMap<>();
    ESScriptUpdate esScriptUpdate = data.getEsScriptUpdate();
    esScriptUpdate.upsert(upsert);
    return upsert;
  }
}
