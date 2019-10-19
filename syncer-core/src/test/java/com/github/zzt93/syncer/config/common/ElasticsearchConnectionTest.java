package com.github.zzt93.syncer.config.common;

import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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

    RestHighLevelClient client = elasticsearchConnection.esClient();

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
    UpdateRequest addRequest = updateRequest(add);
    UpdateRequest metaRequest = updateRequest(meta);
    UpdateRequest removeRequest = updateRequest(remove);

    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(metaRequest);
//    bulkRequest.add(removeRequest);
    bulkRequest.add(removeRequest);
//    bulkRequest.add(addRequest);
//    bulkRequest.add(addRequest);
    bulkRequest.add(metaRequest);
    BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    if (bulkItemResponses.hasFailures()) {
      for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
        System.out.println(itemResponse.getFailure());
      }
      Assert.assertTrue(false);
    }
  }

  private UpdateRequest updateRequest(Script meta) {
    return
        new UpdateRequest("test", "test", "1")
//      client.prepareUpdate("task-0", "task", "13031005")
          .script(meta);
  }

  @Test
  public void name() {
    ArrayList<Long> l = Lists.newArrayList(1L, 2L);
    Assert.assertTrue("should be -1", l.indexOf(1) == -1);
    Assert.assertTrue("should be 0", l.indexOf(1L) == 0);
  }
}