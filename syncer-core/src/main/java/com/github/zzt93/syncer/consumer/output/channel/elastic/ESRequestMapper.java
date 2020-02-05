package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.ESScriptUpdate;
import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.thread.ThreadSafe;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.elastic.ESRequestMapping;
import com.github.zzt93.syncer.consumer.output.channel.mapper.KVMapper;
import org.apache.lucene.search.join.ScoreMode;
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
  // TODO use es7 branch to delete this dependency
  private final AbstractClient client;
  private final KVMapper requestBodyMapper;
  private final Expression indexExpr;
  private final Expression typeExpr;
  private final Expression idExpr;

  ESRequestMapper(AbstractClient client, ESRequestMapping esRequestMapping) {
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
      case WRITE:
        if (esRequestMapping.getNoUseIdForIndex()) {
          return client.prepareIndex(index, type).setSource(requestBodyMapper.map(data));
        }
        return client.prepareIndex(index, type, id).setSource(requestBodyMapper.map(data));
      case DELETE:
        logger.info("Deleting doc from Elasticsearch, may affect performance");
        if (id != null) {
          return client.prepareDelete(index, type, id);
        }
        logger.warn("Deleting doc by query, may affect performance");
        return DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
            .source(index)
            .filter(getFilter(data));
      case UPDATE:
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
      esScriptUpdate.generateScript(code, params);
    }

    return new Script(ScriptType.INLINE, "painless", code.toString(), params);
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
        builder.filter(nestedQuery(key[0], boolQuery().filter(termQuery(entry.getKey(), entry.getValue())), ScoreMode.Avg));
      } else if (key.length == 1) {
        builder.filter(termQuery(entry.getKey(), entry.getValue()));
      } else {
        logger.error("Only support one level nested obj for the time being");
      }
    }
    return builder;
  }

  static Map getUpsert(SyncData data) {
    assert needScript(data);
    HashMap<String, Object> upsert = new HashMap<>();
    ESScriptUpdate esScriptUpdate = data.getEsScriptUpdate();
    esScriptUpdate.upsert(upsert);
    return upsert;
  }
}
