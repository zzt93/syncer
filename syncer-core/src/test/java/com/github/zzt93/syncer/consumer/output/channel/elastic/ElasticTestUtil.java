package com.github.zzt93.syncer.consumer.output.channel.elastic;

import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;

/**
 * @author zzt
 */
@Slf4j
public class ElasticTestUtil {

	public static AbstractClient getRealClient() throws Exception {
		ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
		elasticsearchConnection.setClusterName(System.getProperty("ES_NAME"));
		elasticsearchConnection.setClusterNodes(Lists.newArrayList(System.getProperty("ES_ADDR") + ":9300"));
		return elasticsearchConnection.esClient();
	}

	static AbstractClient getDevClient() throws Exception {

		return new AbstractClient(Settings.EMPTY, null) {
			@Override
			protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
				logger.info("{}, {}", action.name(), request);
				((PlainListenableActionFuture) listener).cancel(true);
			}

			@Override
			public void close() {

			}
		};
	}
}
