package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.config.consumer.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.data.Filter;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.zzt93.syncer.common.data.ESScriptUpdate.BY_ID_SUFFIX;
import static org.junit.Assert.*;

/**
 * @author zzt
 */
public class ESRequestMapperTest {

  public static void mergeToListRemote() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerMergeToList());
  }

  private static List<Object> innerMergeToList() throws Exception {
    List<Object> res = new ArrayList<>();

    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("list", "list");
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    assertEquals("", "index {[list][list][1234], source[{\"roles\":[]}]}",
        builder.toString());
    res.add(builder);


    data = SyncDataTestUtil.write("list", "list");
    data.addField("role", 1381034L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.id("test_id")).mergeToList("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[list][list][1234], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.add(params.roles);', options={}, params={roles=1381034}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);


    data = SyncDataTestUtil.delete("list", "list");
    data.addField("role", 1381034L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.id("test_id")).mergeToList("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[list][list][1234], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(Predicate.isEqual(params.roles));', options={}, params={roles=1381034}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    return res;
  }

  @Test
  public void mergeToList() throws Exception {
    innerMergeToList();
  }

  public static void mergeToListByIdRemote() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerMergeToListById());
  }

  private static List<Object> innerMergeToListById() throws Exception {
    List<Object> res = new ArrayList<>();

    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write();
    data.addField("roles" + BY_ID_SUFFIX, new ArrayList<>());
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    res.add(builder);

    data = SyncDataTestUtil.write();
    data.addField("role", 1381034L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.id("test_id")).mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (!ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles_id.add(params.roles_id); ctx._source.roles.add(params.roles); }', options={}, params={roles_id=1234, roles=1381034}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.delete();
    data.addField("role", 13276746L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.id("test_id")).mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.removeIf(Predicate.isEqual(params.roles_id))) {ctx._source.roles.removeIf(Predicate.isEqual(params.roles)); }', options={}, params={roles_id=1234, roles=13276746}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    return res;
  }

  @Test
  public void mergeToListById() throws Exception {
    innerMergeToListById();
  }

  /**
   * should PUT template first:
   * <pre>
   * PUT _template/nested_template
   * {
   *   "template": "nested*",
   *   "mappings": {
   *     "nested": {
   *       "properties": {
   *         "roles": {
   *           "type": "nested"
   *         }
   *       }
   *     }
   *   }
   * }
   * </pre>
   */
  public static void nestedByParentIdRemote() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerNestedByParentId());
  }

  @Test
  public void nestedByParentId() throws Exception {
    innerNestedByParentId();
  }

  private static List<Object> innerNestedByParentId() throws Exception {
    List<Object> res = new ArrayList<>();

    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("nested", "nested");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    res.add(builder);

    data = SyncDataTestUtil.write("nested", "nested");
    data.addField("role", 1381034L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate(Filter.id("ann_id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=1234, roles={role=1381034, id=1234}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.write("nested", "nested");
    data.addField("role", 2381034L).addField("ann_id", 1L).setId(2345);
    data.esScriptUpdate(Filter.id("ann_id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=2345, roles={role=2381034, id=2345}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.update("nested", "nested");
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate(Filter.id("ann_id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.id));if (target != null) { target.role = params.role;}', options={}, params={role=13276746, id=1234}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.delete("nested", "nested");
    data.addField("role", 13276746L).addField("ann_id", 1L).setId(2345L);
    data.esScriptUpdate(Filter.id("ann_id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.id.equals(params.id)); ', options={}, params={id=2345}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    return res;
  }

  @Test
  public void setFieldNull() throws Exception {
    innerSetFieldNull();
  }

  private static List<Object> innerSetFieldNull() throws Exception {
    List<Object> res = new ArrayList<>();

    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write();
    data.addField("list", Lists.newArrayList(1)).addField("int", 1).addField("str", "1");
    Object builder = mapper.map(data);
    assertEquals("", "index {[test][test][1234], source[{\"str\":\"1\",\"list\":[1],\"int\":1}]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.update();
    data.setFieldNull("int").setFieldNull("str").setFieldNull("list");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc_as_upsert[false], doc[index {[null][null][null], source[{\"str\":null,\"list\":null,\"int\":null}]}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.update();
    data.addField("int", 1381034L).addField("str", "1234").addField("list", Lists.newArrayList(2));

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc_as_upsert[false], doc[index {[null][null][null], source[{\"str\":\"1234\",\"list\":[2],\"int\":1381034}]}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);


    data = SyncDataTestUtil.update();
    data.setFieldNull("int").setFieldNull("str").setFieldNull("list");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc_as_upsert[false], doc[index {[null][null][null], source[{\"str\":null,\"list\":null,\"int\":null}]}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    return res;
  }

  private static void setFieldNullRemote() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerSetFieldNull());
  }

  private static void remoteCheck(RestHighLevelClient client, List<Object> builderList) throws IOException {
    for (Object builder : builderList) {
      BulkRequest bulkRequestBuilder = new BulkRequest();
      if (builder instanceof IndexRequest) {
        bulkRequestBuilder = bulkRequestBuilder.add((IndexRequest) builder);
      } else if (builder instanceof UpdateRequest) {
        bulkRequestBuilder = bulkRequestBuilder.add((UpdateRequest) builder);
      }  else if (builder instanceof DeleteRequest) {
        bulkRequestBuilder = bulkRequestBuilder.add((DeleteRequest) builder);
      } else {
        fail();
      }
      BulkResponse bulkItemResponses = client.bulk(bulkRequestBuilder, RequestOptions.DEFAULT);
      assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));
    }
  }

  @Test
  public void nestedByParentQuery() throws Exception {
    innerNestedByParentQuery();
  }

  /**
   * should PUT template first:
   * <pre>
   * PUT _template/nested_template
   * {
   *   "template": "nested*",
   *   "mappings": {
   *     "nested": {
   *       "properties": {
   *         "roles": {
   *           "type": "nested"
   *         }
   *       }
   *     }
   *   }
   * }
   * </pre>
   */
  public static void nestedByParentQueryRemote() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerNestedByParentQuery());
  }

  private static List<Object> innerNestedByParentQuery() throws Exception {
    List<Object> res = new ArrayList<>();

    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("nested1", "nested");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    res.add(builder);

    data = SyncDataTestUtil.write("nested1", "nested1");
    data.addField("role", 1381034L).addField("nested_id", 1L);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested1][nested1][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=1234, roles={role=1381034, id=1234}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.write("nested1", "nested1");
    data.addField("role", 2381034L).addField("nested_id", 1L).setId(2345);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested1][nested1][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=2345, roles={role=2381034, id=2345}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.update("role", "role").setId(1234L);
    data.setRepo("nested1").setEntity("nested1").addField("title", "b").addField("user_id", 2L);
    data.esScriptUpdate(Filter.fieldId("roles.id")).mergeToNestedById("roles", "title", "user_id");

    builder = mapper.map(data);
    assertEquals("", "{\"size\":1000,\"query\":{\"bool\":{\"filter\":[{\"nested\":{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"roles.id\":{\"value\":1234,\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"path\":\"roles\",\"ignore_unmapped\":false,\"score_mode\":\"avg\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}",
        ((UpdateByQueryRequest)builder).getSearchRequest().source().toString());
    assertEquals("", "update-by-query [nested1] updated with Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.id));if (target != null) { target.user_id = params.user_id;target.title = params.title;}', options={}, params={title=b, user_id=2, id=1234}}",
        ((UpdateByQueryRequest) builder).getDescription());
    res.add(builder);

    data = SyncDataTestUtil.delete("role", "role");
    data.setRepo("nested1").setEntity("nested1");
    data.esScriptUpdate(Filter.fieldId("roles.id")).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "{\"size\":1000,\"query\":{\"bool\":{\"filter\":[{\"nested\":{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"roles.id\":{\"value\":1234,\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"path\":\"roles\",\"ignore_unmapped\":false,\"score_mode\":\"avg\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}",
        ((UpdateByQueryRequest)builder).getSearchRequest().source().toString());
    assertEquals("", "update-by-query [nested1] updated with Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.id.equals(params.id)); ', options={}, params={id=1234}}",
        ((UpdateByQueryRequest) builder).getDescription());
    res.add(builder);

    return res;
  }

  private static List<Object> innerNestedByQuery() throws Exception {
    List<Object> res = new ArrayList<>();

    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("nested2", "nested2");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    res.add(builder);

    data = SyncDataTestUtil.write("nested2", "nested2");
    data.addField("username", "1").addField("user_id", 1L).addField("nested_id", 1L).setId(1234L);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedById("roles", "user_id", "username");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=1234, roles={user_id=1, id=1234, username=1}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.write("nested2", "nested2");
    data.addField("username", "1").addField("user_id", 1L).addField("nested_id", 1L).setId(2345);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedById("roles", "user_id", "username");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=2345, roles={user_id=1, id=2345, username=1}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.write("nested2", "nested2");
    data.addField("username", "2").addField("user_id", 2L).addField("nested_id", 1L).setId(3456);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedById("roles", "user_id", "username");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=3456, roles={user_id=2, id=3456, username=2}}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    /**/

    data = SyncDataTestUtil.update("nested2", "nested2");
    data.addField("username", "11").addField("user_id", 1).addField("nested_id", 1L);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedByQuery("roles", Filter.of("user_id", "user_id"), "username");
    data.removeField("user_id");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.user_id.equals(params.user_id));if (target != null) { target.username = params.username;}', options={}, params={user_id=1, username=11}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    data = SyncDataTestUtil.update("user", "user").setId(1);
    data.setRepo("nested2").setEntity("nested2").addField("username", "b");
    data.esScriptUpdate(Filter.fieldId("roles.user_id")).mergeToNestedByQuery("roles", Filter.fieldId("user_id"), "username");

    builder = mapper.map(data);
    assertEquals("", "{\"size\":1000,\"query\":{\"bool\":{\"filter\":[{\"nested\":{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"roles.user_id\":{\"value\":1,\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"path\":\"roles\",\"ignore_unmapped\":false,\"score_mode\":\"avg\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}",
        ((UpdateByQueryRequest)builder).getSearchRequest().source().toString());
    assertEquals("", "update-by-query [nested2] updated with Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.user_id.equals(params.user_id));if (target != null) { target.username = params.username;}', options={}, params={user_id=1, username=b}}",
        ((UpdateByQueryRequest) builder).getDescription());
    res.add(builder);


    data = SyncDataTestUtil.delete("nested2", "nested2");
    data.addField("user_id", 2).addField("nested_id", 1L).setId(2345L);
    data.esScriptUpdate(Filter.id("nested_id")).mergeToNestedByQuery("roles", Filter.of("user_id", "user_id"));
    data.removeField("user_id");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], doc_as_upsert[false], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.user_id.equals(params.user_id)); ', options={}, params={user_id=2}}], scripted_upsert[false], detect_noop[true]}",
        builder.toString());
    res.add(builder);

    return res;
  }

  @Test
  public void nestedByQuery() throws Exception {
    innerNestedByQuery();
  }

  public static void nestedByQueryRemote() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerNestedByQuery());
  }

  @Test
  public void nestedWithExtraQuery() throws Exception {
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.update("role", "role").setId(1234L);
    data.setRepo("nested3").setEntity("nested3").addField("title", "b").addField("user_id", 2L);
    data.extraQuery("user", "user").filter("_id", 2L).select("username").addField("username");
    data.esScriptUpdate(Filter.fieldId("roles.id")).mergeToNestedById("roles", "title", "user_id", "username");

    Object builder = mapper.map(data);
    assertEquals("", "{\"size\":1000,\"query\":{\"bool\":{\"filter\":[{\"nested\":{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"roles.id\":{\"value\":1234,\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"path\":\"roles\",\"ignore_unmapped\":false,\"score_mode\":\"avg\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}",
        ((UpdateByQueryRequest)builder).getSearchRequest().source().toString());
    assertEquals("", "update-by-query [nested3] updated with Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.id));if (target != null) { target.user_id = params.user_id;target.title = params.title;target.username = params.username;}', options={}, params={id=1234, title=b, user_id=2, username=null}}",
        ((UpdateByQueryRequest) builder).getDescription());
  }

  @Test
  public void retryOnConflict() throws Exception {
    SyncData data = SyncDataTestUtil.update("role", "role").setId(1234L);
    RestHighLevelClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());
    UpdateRequest update = (UpdateRequest) mapper.map(data);
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(update);
    // new String(Files.read(RequestConverters.bulk(bulkRequest).entity.getContent(), 90))
    assertEquals("", "update {[role][role][1234], doc_as_upsert[false], doc[index {[null][null][null], source[{}]}], scripted_upsert[false], detect_noop[true]}", update.toString());
  }

  public static void main(String[] args) throws Exception {
//    nestedByParentIdRemote();
//    mergeToListRemote();
//    setFieldNullRemote();
  }
}