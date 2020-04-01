package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Assert;
import org.junit.Test;

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

  @Test
  public void toStr() throws Exception {
    UpdateRequest request = new UpdateRequest("test", "type1", "1")
        .script(mockInlineScript("ctx._source.body = \"foo\""));
    Assert.assertEquals("update {[test][type1][1], doc_as_upsert[false], script[Script{type=inline, lang='mock', idOrCode='ctx._source.body = \"foo\"', options={}, params={}}], scripted_upsert[false], detect_noop[true]}",
        request.toString());
    request = new UpdateRequest("test", "type1", "1").doc("{\"body\": \"bar\"}", XContentType.JSON);
    Assert.assertEquals("update {[test][type1][1], doc_as_upsert[false], doc[index {[null][null][null], source[{\"body\": \"bar\"}]}], scripted_upsert[false], detect_noop[true]}",
        request.toString());
  }

  /**
   * https://discuss.elastic.co/t/java-api-plainless-script-indexof-give-wrong-answer/139016/7
   */
  public void scriptIndexOf() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getIntClient();

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
    UpdateRequest addRequest = updateRequest(client, add);
    UpdateRequest metaRequest = updateRequest(client, meta);
    UpdateRequest removeRequest = updateRequest(client, remove);

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

  private UpdateRequest updateRequest(RestHighLevelClient client, Script meta) {
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