package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.util.EsTypeUtil;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * @author zzt
 */
public class ElasticsearchJavaAPIScriptTest {

  public static final long LONG_ID = Integer.MAX_VALUE;
  public static final String USERS = "users";

  private static Script mockInlineScript(final String script) {
    return new Script(ScriptType.INLINE, "mock", script, emptyMap());
  }

  public static void main(String[] args) throws Exception {
//    scriptIndexOf();
    scriptEqual();
  }

  /**
   * https://discuss.elastic.co/t/java-api-plainless-script-indexof-give-wrong-answer/139016/7
   */
  public static void scriptIndexOf() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();

    HashMap<String, Object> params = new HashMap<>();
    params.put("users", LONG_ID);
    Script add = new Script(ScriptType.INLINE, "painless",
        "ctx._source.users.add(params.users);",
        params);
    Script meta = new Script(ScriptType.INLINE, "painless",
        "ctx._source.t1 = params.users; ctx._source.i11= ctx._source.users.indexOf(params.users); ctx._source.i21= ctx._source.users.lastIndexOf(params.users);",
        params);
    Script remove = new Script(ScriptType.INLINE, "painless",
        "ctx._source.users.remove(ctx._source.users.indexOf(params.users));",
        params);

    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(indexRequest(client, getSource()));
    bulkRequest.add(updateRequest(client, add));
    bulkRequest.add(updateRequest(client, meta));
    bulkRequest.add(updateRequest(client, remove));
    bulkRequest.add(updateRequest(client, meta));
    BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
    if (bulkItemResponses.hasFailures()) {
      for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
        System.out.println(itemResponse.getFailure());
      }
//      Assert.fail();
    }
  }

  private static HashMap<String, Object> getSource() {
    HashMap<String, Object> sources = new HashMap<>();
    sources.put(USERS, Lists.newArrayList());
    sources.put("nested", Lists.newArrayList());
    return sources;
  }

  public static void scriptEqual() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();

    HashMap<String, Object> params = new HashMap<>();
    HashMap<String, Object> user0 = new HashMap<>();
    user0.put("id", LONG_ID);
    params.put("user0", user0);
    Script add = new Script(ScriptType.INLINE, "painless",
        "ctx._source.nested.add(params.user0);",
        params);

    params = new HashMap<>();
//    params.put("userId0", LONG_ID);
    params.put("userId0", EsTypeUtil.scriptConvert(LONG_ID));
    params.put("user0", user0);
    Script meta = new Script(ScriptType.INLINE, "painless",
        "if (ctx._source.nested == null) {ctx._source.nested = [];}if (ctx._source.nested.find(e -> e.id.equals(params.userId0)) == null) {\n" +
            "ctx._source.nested.add(params.user0);\n" +
            "}",
        params);

    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(indexRequest(client, getSource()));
    bulkRequest.add(updateRequest(client, add));
    bulkRequest.add(updateRequest(client, meta));
    BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
    if (bulkItemResponses.hasFailures()) {
      for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
        System.out.println(itemResponse.getFailure());
      }
//      Assert.fail();
    }
  }

  private static UpdateRequest updateRequest(AbstractClient client, Script meta) {
    return client.prepareUpdate("test", "test", "1").setScript(meta).request();
  }

  private static IndexRequest indexRequest(AbstractClient client, Map<String, Object> source) {
    return client.prepareIndex("test", "test", "1").setSource(source).request();
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

  @Test
  public void name() {
    ArrayList<Long> l = Lists.newArrayList(1L, 2L);
    Assert.assertEquals("should be -1", l.indexOf(1), -1);
    Assert.assertEquals("should be 0", 0, l.indexOf(1L));
  }

}