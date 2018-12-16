package com.github.zzt93.syncer.config.common;

import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author zzt
 */
public class ElasticsearchConnectionTest {

  /**
   * https://discuss.elastic.co/t/java-api-plainless-script-indexof-give-wrong-answer/139016/7
   */
  public void scriptIndexOf() throws Exception {
    ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
    elasticsearchConnection.setClusterName("searcher-integration");
    elasticsearchConnection.setClusterNodes(Lists.newArrayList("192.168.1.100:9300"));

    AbstractClient client = elasticsearchConnection.esClient();

    HashMap<String, Object> params = new HashMap<>();
    params.put("users", 540722L);
//    params.put("users", 540722);
    Script meta = new Script(ScriptType.INLINE, "painless",
        "ctx._source.t1 = params.users; ctx._source.i11= ctx._source.users.indexOf(params.users); ctx._source.i21= ctx._source.users.lastIndexOf(params.users);",
        params);
    Script add = new Script(ScriptType.INLINE, "painless",
        "ctx._source.users.add(params.users);",
        params);
    Script remove = new Script(ScriptType.INLINE, "painless",
        "ctx._source.users.remove(ctx._source.users.indexOf(params.users));",
        params);
    UpdateRequestBuilder addRequest = updateRequest(client, add);
    UpdateRequestBuilder metaRequest = updateRequest(client, meta);
    UpdateRequestBuilder removeRequest = updateRequest(client, remove);

    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(metaRequest.request());
//    bulkRequest.add(removeRequest.request());
    bulkRequest.add(removeRequest.request());
//    bulkRequest.add(addRequest.request());
//    bulkRequest.add(addRequest.request());
    bulkRequest.add(metaRequest.request());
    BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
    if (bulkItemResponses.hasFailures()) {
      for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
        System.out.println(itemResponse.getFailure());
      }
      Assert.assertTrue(false);
    }
  }

  private UpdateRequestBuilder updateRequest(AbstractClient client, Script meta) {
    return
        client.prepareUpdate("test", "test", "1")
//      client.prepareUpdate("task-0", "task", "13031005")
          .setScript(meta);
  }

  @Test
  public void name() {
    ArrayList<Long> l = Lists.newArrayList(1L, 2L);
    Assert.assertTrue("should be -1", l.indexOf(1) == -1);
    Assert.assertTrue("should be 0", l.indexOf(1L) == 0);
  }
}