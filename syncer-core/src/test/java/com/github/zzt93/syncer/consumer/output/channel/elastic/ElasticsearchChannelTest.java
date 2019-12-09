package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static java.util.Collections.emptyMap;

/**
 * @author zzt
 */
public class ElasticsearchChannelTest {

  private static Script mockInlineScript(final String script) {
    return new Script(ScriptType.INLINE, "mock", script, emptyMap());
  }
  private XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
    return xContent.createParser(xContentRegistry(), data);
  }
  private NamedXContentRegistry xContentRegistry() {
    return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
  }

  @Test
  public void toStr() throws Exception {
    UpdateRequest request = new UpdateRequest("test", "type1", "1")
        .script(mockInlineScript("ctx._source.body = \"foo\""));
    Assert.assertEquals(ElasticsearchChannel.toString(request),
        "update {[test][type1][1], script[Script{type=inline, lang='mock', idOrCode='ctx._source.body = \"foo\"', options={}, params={}}], detect_noop[true]}");
    request = new UpdateRequest("test", "type1", "1").fromXContent(
        createParser(JsonXContent.jsonXContent, new BytesArray("{\"doc\": {\"body\": \"bar\"}}")));
    Assert.assertEquals(ElasticsearchChannel.toString(request),
        "update {[test][type1][1], doc[index {[null][null][null], source[{\"body\":\"bar\"}]}], detect_noop[true]}");
  }

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