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

  @Test
  public void map() throws Exception {

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
    data.esScriptUpdate().mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (!ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles_id.add(params.roles_id); ctx._source.roles.add(params.roles); }', options={}, params={roles_id=1, roles=1381034}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.update();
    data.getBefore().put("role", 1381034L);
    data.addField("role", 13276746L);
    data.esScriptUpdate().mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.contains(params.roles_id)) {ctx._source.roles.set(ctx._source.roles.indexOf(params.roles_before), params.roles); }', options={}, params={roles_before=1381034, roles_id=1, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));

    data = SyncDataTestUtil.delete();
    data.addField("role", 13276746L);
    data.esScriptUpdate().mergeToListById("roles", "role");

    builder = mapper.map(data);
    assertEquals("", "update {[test][test][1], script[Script{type=inline, lang='painless', idOrCode='if (ctx._source.roles_id.removeIf(Predicate.isEqual(params.roles_id))) {ctx._source.roles.removeIf(Predicate.isEqual(params.roles)); }', options={}, params={roles_id=1, roles=13276746}}], detect_noop[true]}",
        ElasticsearchChannel.toString(((UpdateRequestBuilder) builder).request()));

    bulkRequestBuilder = client.prepareBulk().add((UpdateRequestBuilder) builder);
    bulkItemResponses = bulkRequestBuilder.execute().get();
    assertFalse(Arrays.stream(bulkItemResponses.getItems()).anyMatch(BulkItemResponse::isFailed));
  }
}