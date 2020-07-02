package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.config.common.HttpConnection;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author zzt
 */
@Slf4j
public class EtcdBasedFile implements MetaFile {

	private final KV kvClient;
	private final ByteSequence path;

	/**
	 * @param path instanceId + consumerId + outputIdentifier
	 */
	public EtcdBasedFile(HttpConnection connection, String path) {
    //		"http://localhost:2379"
		Client client = Client.builder().endpoints(connection.toConnectionUrl(null)).build();
		kvClient = client.getKVClient();
		this.path = ByteSequence.from(path.getBytes());
	}

	@Override
	public boolean isExists() {
		return true;
	}

	@Override
	public void createFile() {

	}

	@Override
	public void initFile() {

	}

	@Override
	public byte[] readData() throws IOException {
		CompletableFuture<GetResponse> getFuture = kvClient.get(path);
		GetResponse response = null;
		try {
			response = getFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("", e);
		}
//		return response.getKvs();
		return null;
	}

	@Override
	public void putBytes(byte[] bytes) throws IOException {
		ByteSequence value = ByteSequence.from("test_value".getBytes());
		try {
			kvClient.put(path, value).get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("", e);
		}
	}


}
