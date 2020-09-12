package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.config.common.EtcdConnection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author zzt
 */
@Slf4j
public class EtcdBasedFile implements MetaFile {
	private final EtcdConnection connection;

	private final KV kvClient;
	private final ByteSequence key;


	public EtcdBasedFile(EtcdConnection connection) {
		this.connection = connection;
		//		"http://localhost:2379"
		Client client = Client.builder().endpoints(connection.toConnectionUrl(null)).build();
		kvClient = client.getKVClient();
		this.key = ByteSequence.from(connection.getKey().getBytes());
	}

	@Override
	public boolean isExists() {
		return true;
	}

	@Override
	public void createFileAndInitFile() {
		// no need to create
		// no need to init
	}

	@Override
	public AckMetaData readData() throws IOException {
		CompletableFuture<GetResponse> getFuture = kvClient.get(key);
		GetResponse response = null;
		try {
			response = getFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("Fail to fetch {} {}", kvClient, key, e);
			throw new InvalidConfigException("Fail to fetch");
		}
		List<KeyValue> kvs = response.getKvs();
		if (kvs.size() == 0) {
			return AckMetaData.empty();
		}
		return new AckMetaData(kvs.get(kvs.size() - 1).getValue().getBytes());
	}

	@Override
	public void putBytes(byte[] bytes) throws IOException {
		ByteSequence value = ByteSequence.from(bytes);
		try {
			PutResponse putResponse = kvClient.put(key, value).get();
			if (putResponse.hasPrevKv()) {
				KeyValue prevKv = putResponse.getPrevKv();
				log.debug("{}, {}", prevKv.getKey().toString(StandardCharsets.UTF_8), prevKv.getValue().toString(StandardCharsets.UTF_8));
			}
		} catch (InterruptedException | ExecutionException e) {
			log.error("", e);
		}
	}


	@Override
	public String toString() {
		return "EtcdBasedFile{" +
				"connection=" + connection.toConnectionUrl(null) +
				", key=" + connection.getKey() +
				'}';
	}
}
