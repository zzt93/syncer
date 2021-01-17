package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.config.consumer.output.elastic.Elasticsearch;
import com.github.zzt93.syncer.data.es.ESDocKey;
import com.github.zzt93.syncer.data.es.Filter;
import com.github.zzt93.syncer.data.es.SyncDataKey;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.github.zzt93.syncer.common.data.ESScriptUpdate.*;
import static org.junit.Assert.*;

/**
 * @author zzt
 */
public class ESRequestMapperTest {

  public static void mergeToListRemote() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerMergeToList());
  }

  private static List<Object> innerMergeToList() throws Exception {
    List<Object> res = new ArrayList<>();

    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("list", "list");
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    assertEquals("", "index {[list][list][1234], source[{\"roles\":[]}]}",
        ((IndexRequestBuilder) builder).request().toString());
    res.add(builder);


    data = SyncDataTestUtil.write("list", "list");
    data.addField("role", 1381034L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("test_id"))).mergeToList("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[list][list][1234], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.add(params.roles);', options={}, params={roles=1381034}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);


    data = SyncDataTestUtil.delete("list", "list");
    data.addField("role", 1381034L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("test_id"))).mergeToList("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[list][list][1234], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(Predicate.isEqual(params.roles));', options={}, params={roles=1381034}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    return res;
  }

  @Test
  public void mergeToList() throws Exception {
    innerMergeToList();
  }

  public static void mergeToListByIdRemote() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerMergeToListById());
  }

  private static List<Object> innerMergeToListById() throws Exception {
    List<Object> res = new ArrayList<>();

    AbstractClient client = ElasticTestUtil.getDevClient();
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
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("test_id"))).mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], script[Script{type=inline, lang='painless', idOrCode='if (!ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles_id.add(params.roles_id); ctx._source.roles.add(params.roles); }', options={}, params={roles_id=1234, roles=1381034}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.delete();
    data.addField("role", 13276746L);
    data.addField("test_id", 1234L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("test_id"))).mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.removeIf(Predicate.isEqual(params.roles_id))) {ctx._source.roles.removeIf(Predicate.isEqual(params.roles)); }', options={}, params={roles_id=1234, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
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
    AbstractClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerNestedByParentId());
  }

  @Test
  public void nestedByParentId() throws Exception {
    innerNestedByParentId();
  }

  private static List<Object> innerNestedByParentId() throws Exception {
    List<Object> res = new ArrayList<>();

    AbstractClient client = ElasticTestUtil.getDevClient();
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
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("ann_id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=1234, roles={role=1381034, id=1234}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.write("nested", "nested");
    data.addField("role", 2381034L).addField("ann_id", 1L).setId(2345);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("ann_id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=2345, roles={role=2381034, id=2345}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.update("nested", "nested");
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("ann_id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], script[Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.id));if (target != null) { target.role = params.role;}', options={}, params={role=13276746, id=1234}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.delete("nested", "nested");
    data.addField("role", 13276746L).addField("ann_id", 1L).setId(2345L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("ann_id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested][nested][1], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.id.equals(params.id)); ', options={}, params={id=2345}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    return res;
  }

  @Test
  public void setFieldNull() throws Exception {
    innerSetFieldNull();
  }

  private static List<Object> innerSetFieldNull() throws Exception {
    List<Object> res = new ArrayList<>();

    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write();
    data.addField("str", "1").addField("list", Lists.newArrayList(1)).addField("int", 1);
    Object builder = mapper.map(data);
    assertEquals("", "index {[test][test][1234], source[{\"str\":\"1\",\"list\":[1],\"int\":1}]}",
        ((IndexRequestBuilder) builder).request().toString());
    res.add(builder);

    data = SyncDataTestUtil.update();
    data.setFieldNull("str").setFieldNull("list").setFieldNull("int");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc[index {[null][null][null], source[{\"str\":null,\"list\":null,\"int\":null}]}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.update();
    data.addField("str", "1234").addField("list", Lists.newArrayList(2)).addField("int", 1381034L);

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc[index {[null][null][null], source[{\"str\":\"1234\",\"list\":[2],\"int\":1381034}]}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);


    data = SyncDataTestUtil.update();
    data.setFieldNull("str").setFieldNull("list").setFieldNull("int");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1234], doc[index {[null][null][null], source[{\"str\":null,\"list\":null,\"int\":null}]}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    return res;
  }

  private static void setFieldNullRemote() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerSetFieldNull());
  }

  private static void remoteCheck(AbstractClient client, List<Object> builderList) throws ExecutionException, InterruptedException {
    for (Object builder : builderList) {
      BulkRequestBuilder bulkRequestBuilder = null;
      if (builder instanceof IndexRequestBuilder) {
        bulkRequestBuilder = client.prepareBulk().add((IndexRequestBuilder) builder);
      } else if (builder instanceof UpdateRequestBuilder) {
        bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
      }  else if (builder instanceof DeleteRequestBuilder) {
        bulkRequestBuilder = client.prepareBulk().add((DeleteRequestBuilder) builder);
      } else {
        fail();
      }
      BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
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
    AbstractClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerNestedByParentQuery());
  }

  private static List<Object> innerNestedByParentQuery() throws Exception {
    List<Object> res = new ArrayList<>();

    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("nested1", "nested");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    res.add(builder);

    data = SyncDataTestUtil.write("nested1", "nested1");
    data.addField("role", 1381034L).addField("nested_id", 1L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested1][nested1][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=1234, roles={role=1381034, id=1234}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.write("nested1", "nested1");
    data.addField("role", 2381034L).addField("nested_id", 1L).setId(2345);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[nested1][nested1][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=2345, roles={role=2381034, id=2345}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.update("role", "role").setId(1234L);
    data.setRepo("nested1").setEntity("nested1").addField("title", "b").addField("user_id", 2L);
    data.esScriptUpdate(Filter.syncId(ESDocKey.of("roles.id"))).mergeToNestedById("roles", "title", "user_id");

    builder = mapper.map(data);
    assertEquals("", "{\n" +
            "  \"size\" : 1000,\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"filter\" : [\n" +
            "        {\n" +
            "          \"nested\" : {\n" +
            "            \"query\" : {\n" +
            "              \"bool\" : {\n" +
            "                \"filter\" : [\n" +
            "                  {\n" +
            "                    \"term\" : {\n" +
            "                      \"roles.id\" : {\n" +
            "                        \"value\" : 1234,\n" +
            "                        \"boost\" : 1.0\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                ],\n" +
            "                \"disable_coord\" : false,\n" +
            "                \"adjust_pure_negative\" : true,\n" +
            "                \"boost\" : 1.0\n" +
            "              }\n" +
            "            },\n" +
            "            \"path\" : \"roles\",\n" +
            "            \"ignore_unmapped\" : false,\n" +
            "            \"score_mode\" : \"avg\",\n" +
            "            \"boost\" : 1.0\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}",
        ((AbstractBulkByScrollRequestBuilder)builder).source().toString());
    assertEquals("", "update-by-query [nested1] updated with Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.id));if (target != null) { target.user_id = params.user_id;target.title = params.title;}', options={}, params={title=b, user_id=2, id=1234}}",
        ((UpdateByQueryRequestBuilder) builder).request().toString());
    res.add(builder);

    data = SyncDataTestUtil.delete("role", "role");
    data.setRepo("nested1").setEntity("nested1");
    data.esScriptUpdate(Filter.syncId(ESDocKey.of("roles.id"))).mergeToNestedById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "{\n" +
            "  \"size\" : 1000,\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"filter\" : [\n" +
            "        {\n" +
            "          \"nested\" : {\n" +
            "            \"query\" : {\n" +
            "              \"bool\" : {\n" +
            "                \"filter\" : [\n" +
            "                  {\n" +
            "                    \"term\" : {\n" +
            "                      \"roles.id\" : {\n" +
            "                        \"value\" : 1234,\n" +
            "                        \"boost\" : 1.0\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                ],\n" +
            "                \"disable_coord\" : false,\n" +
            "                \"adjust_pure_negative\" : true,\n" +
            "                \"boost\" : 1.0\n" +
            "              }\n" +
            "            },\n" +
            "            \"path\" : \"roles\",\n" +
            "            \"ignore_unmapped\" : false,\n" +
            "            \"score_mode\" : \"avg\",\n" +
            "            \"boost\" : 1.0\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}",
        ((AbstractBulkByScrollRequestBuilder)builder).source().toString());
    assertEquals("", "update-by-query [nested1] updated with Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.id.equals(params.id)); ', options={}, params={id=1234}}",
        ((UpdateByQueryRequestBuilder) builder).request().toString());
    res.add(builder);

    return res;
  }

  private static List<Object> innerNestedByQuery() throws Exception {
    List<Object> res = new ArrayList<>();

    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("nested2", "nested2");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    res.add(builder);

    data = SyncDataTestUtil.write("nested2", "nested2");
    data.addField("username", "1").addField("user_id", 1L).addField("nested_id", 1L).setId(1234L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedById("roles", "user_id", "username");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=1234, roles={user_id=1, id=1234, username=1}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.write("nested2", "nested2");
    data.addField("username", "1").addField("user_id", 1L).addField("nested_id", 1L).setId(2345);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedById("roles", "user_id", "username");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=2345, roles={user_id=1, id=2345, username=1}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.write("nested2", "nested2");
    data.addField("username", "2").addField("user_id", 2L).addField("nested_id", 1L).setId(3456);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedById("roles", "user_id", "username");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={id=3456, roles={user_id=2, id=3456, username=2}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    /**/

    data = SyncDataTestUtil.update("nested2", "nested2");
    data.addField("username", "11").addField("user_id", 1).addField("nested_id", 1L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedByQuery("roles", Filter.of(ESDocKey.of("user_id"), SyncDataKey.of("user_id")), "username");
    data.removeField("user_id");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], script[Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.user_id.equals(params.user_id));if (target != null) { target.username = params.username;}', options={}, params={user_id=1, username=11}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    data = SyncDataTestUtil.update("user", "user").setId(1);
    data.setRepo("nested2").setEntity("nested2").addField("username", "b");
    data.esScriptUpdate(Filter.syncId(ESDocKey.of("roles.user_id"))).mergeToNestedByQuery("roles", Filter.syncId(ESDocKey.of("user_id")), "username");

    builder = mapper.map(data);
    assertEquals("", "{\n" +
            "  \"size\" : 1000,\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"filter\" : [\n" +
            "        {\n" +
            "          \"nested\" : {\n" +
            "            \"query\" : {\n" +
            "              \"bool\" : {\n" +
            "                \"filter\" : [\n" +
            "                  {\n" +
            "                    \"term\" : {\n" +
            "                      \"roles.user_id\" : {\n" +
            "                        \"value\" : 1,\n" +
            "                        \"boost\" : 1.0\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                ],\n" +
            "                \"disable_coord\" : false,\n" +
            "                \"adjust_pure_negative\" : true,\n" +
            "                \"boost\" : 1.0\n" +
            "              }\n" +
            "            },\n" +
            "            \"path\" : \"roles\",\n" +
            "            \"ignore_unmapped\" : false,\n" +
            "            \"score_mode\" : \"avg\",\n" +
            "            \"boost\" : 1.0\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}",
        ((AbstractBulkByScrollRequestBuilder)builder).source().toString());
    assertEquals("", "update-by-query [nested2] updated with Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.user_id.equals(params.user_id));if (target != null) { target.username = params.username;}', options={}, params={user_id=1, username=b}}",
        ((UpdateByQueryRequestBuilder) builder).request().toString());
    res.add(builder);


    data = SyncDataTestUtil.delete("nested2", "nested2");
    data.addField("user_id", 2).addField("nested_id", 1L).setId(2345L);
    data.esScriptUpdate(Filter.esId(SyncDataKey.of("nested_id"))).mergeToNestedByQuery("roles", Filter.of(ESDocKey.of("user_id"), SyncDataKey.of("user_id")));
    data.removeField("user_id");

    builder = mapper.map(data);
    assertEquals("", "update {[nested2][nested2][1], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.user_id.equals(params.user_id)); ', options={}, params={user_id=2}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));
    res.add(builder);

    return res;
  }

  @Test
  public void nestedByQuery() throws Exception {
    innerNestedByQuery();
  }

  public static void nestedByQueryRemote() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    remoteCheck(client, innerNestedByQuery());
  }

  @Test
  public void nestedWithExtraQuery() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.update("role", "role").setId(1234L);
    data.setRepo("nested3").setEntity("nested3").addField("title", "b").addField("user_id", 2L);
    data.extraQuery("user", "user").filter("_id", -1L).select("username").addField("username");
    data.esScriptUpdate(Filter.syncId(ESDocKey.of("roles.id"))).mergeToNestedById("roles", "title", "user_id", "username");

    Object builder = mapper.map(data);
    assertEquals("", "{\n" +
            "  \"size\" : 1000,\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"filter\" : [\n" +
            "        {\n" +
            "          \"nested\" : {\n" +
            "            \"query\" : {\n" +
            "              \"bool\" : {\n" +
            "                \"filter\" : [\n" +
            "                  {\n" +
            "                    \"term\" : {\n" +
            "                      \"roles.id\" : {\n" +
            "                        \"value\" : 1234,\n" +
            "                        \"boost\" : 1.0\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                ],\n" +
            "                \"disable_coord\" : false,\n" +
            "                \"adjust_pure_negative\" : true,\n" +
            "                \"boost\" : 1.0\n" +
            "              }\n" +
            "            },\n" +
            "            \"path\" : \"roles\",\n" +
            "            \"ignore_unmapped\" : false,\n" +
            "            \"score_mode\" : \"avg\",\n" +
            "            \"boost\" : 1.0\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}",
        ((AbstractBulkByScrollRequestBuilder)builder).source().toString());
    assertEquals("", "update-by-query [nested3] updated with Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.id));if (target != null) { target.user_id = params.user_id;target.title = params.title;target.username = params.username;}', options={}, params={id=1234, title=b, user_id=2, username=null}}",
        ((UpdateByQueryRequestBuilder) builder).request().toString());
  }

  public static void main(String[] args) throws Exception {
//    nestedByParentIdRemote();
//    mergeToListRemote();
//    setFieldNullRemote();
  }
}