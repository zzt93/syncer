package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.config.consumer.output.elastic.Elasticsearch;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author zzt
 */
public class ESRequestMapperTest {

  public void mergeToListByIdRemote() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write();
    data.addField("roles_id", new ArrayList<>());
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);

    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().add((IndexRequestBuilder) builder);
    BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.write();
    data.addField("role", 1381034L);
    data.addField("test_id", 1L);
    data.esScriptUpdate().mergeToListById("roles", "test_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (!ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles_id.add(params.roles_id); ctx._source.roles.add(params.roles); }', options={}, params={roles_id=1234, roles=1381034}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.update();
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.addField("test_id", 1L);
    data.esScriptUpdate().mergeToListById("roles", "test_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles.set(ctx._source.roles.indexOf(params.roles_before), params.roles); }', options={}, params={roles_before=1381034, roles_id=1234, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.delete();
    data.addField("role", 13276746L);
    data.addField("test_id", 1L);
    data.esScriptUpdate().mergeToListById("roles", "test_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.removeIf(Predicate.isEqual(params.roles_id))) {ctx._source.roles.removeIf(Predicate.isEqual(params.roles)); }', options={}, params={roles_id=1234, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));
  }

  @Test
  public void mergeToListById() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write();
    data.addField("roles_id", new ArrayList<>());
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);
    assertEquals("", "index {[test][test][1234], source[{\"roles_id\":[],\"roles\":[]}]}",
        ((IndexRequestBuilder) builder).request().toString());

    data = SyncDataTestUtil.write();
    data.addField("role", 1381034L);
    data.addField("test_id", 1L);
    data.esScriptUpdate().mergeToListById("roles", "test_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (!ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles_id.add(params.roles_id); ctx._source.roles.add(params.roles); }', options={}, params={roles_id=1234, roles=1381034}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));


    data = SyncDataTestUtil.update();
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.addField("test_id", 1L);
    data.esScriptUpdate().mergeToListById("roles", "test_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles.set(ctx._source.roles.indexOf(params.roles_before), params.roles); }', options={}, params={roles_before=1381034, roles_id=1234, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));


    data = SyncDataTestUtil.delete();
    data.addField("role", 13276746L);
    data.addField("test_id", 1L);
    data.esScriptUpdate().mergeToListById("roles", "test_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.removeIf(Predicate.isEqual(params.roles_id))) {ctx._source.roles.removeIf(Predicate.isEqual(params.roles)); }', options={}, params={roles_id=1234, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

  }

  public void nestedByIdRemote() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("ann", "ann");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);

    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().add((IndexRequestBuilder) builder);
    BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.write("ann", "ann");
    data.addField("role", 1381034L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate().mergeToNestedById("roles", "ann_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[ann][ann][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.roles_id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={roles_id=1234, roles={role=1381034, id=1234}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.update("ann", "ann");
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate().mergeToNestedById("roles", "ann_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[ann][ann][1], script[Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.roles_id));if (target != null) { target.role = params.role;target.id = params.id;}', options={}, params={role=13276746, id=1234, roles_id=1234}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.delete("ann", "ann");
    data.addField("role", 13276746L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate().mergeToNestedById("roles", "ann_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[ann][ann][1], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.id.equals(params.roles_id)); ', options={}, params={roles_id=1234}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));
  }

  @Test
  public void nestedById() throws Exception {
    AbstractClient client = ElasticTestUtil.getDevClient();
    Elasticsearch elasticsearch = new Elasticsearch();
    ESRequestMapper mapper = new ESRequestMapper(client, elasticsearch.getRequestMapping());

    SyncData data = SyncDataTestUtil.write("ann", "ann");
    data.setId(1L);
    data.addField("roles", new ArrayList<>());
    Object builder = mapper.map(data);

    data = SyncDataTestUtil.write("ann", "ann");
    data.addField("role", 1381034L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate().mergeToNestedById("roles", "ann_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[ann][ann][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles.find(e -> e.id.equals(params.roles_id)) == null) {  ctx._source.roles.add(params.roles);}', options={}, params={roles_id=1234, roles={role=1381034, id=1234}}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    data = SyncDataTestUtil.update("ann", "ann");
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate().mergeToNestedById("roles", "ann_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[ann][ann][1], script[Script{type=inline, lang='painless', idOrCode='def target = ctx._source.roles.find(e -> e.id.equals(params.roles_id));if (target != null) { target.role = params.role;target.id = params.id;}', options={}, params={role=13276746, id=1234, roles_id=1234}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    data = SyncDataTestUtil.delete("ann", "ann");
    data.addField("role", 13276746L);
    data.addField("ann_id", 1L);
    data.esScriptUpdate().mergeToNestedById("roles", "ann_id", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[ann][ann][1], script[Script{type=inline, lang='painless', idOrCode='ctx._source.roles.removeIf(e -> e.id.equals(params.roles_id)); ', options={}, params={roles_id=1234}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

  }

}