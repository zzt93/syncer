package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.config.common.EtcdConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

//import io.etcd.jetcd.ByteSequence;
//import io.etcd.jetcd.Client;
//import io.etcd.jetcd.KV;
//import io.etcd.jetcd.kv.GetResponse;

/**
 * @author zzt
 */
@Slf4j
public class EtcdBasedFile implements MetaFile {
	private final EtcdConnection connection;

//	private final KV kvClient;
//	private final ByteSequence key;


	public EtcdBasedFile(EtcdConnection connection) {
		this.connection = connection;
		//		"http://localhost:2379"
//		Client client = Client.builder().endpoints(connection.toConnectionUrl(null)).build();
//		kvClient = client.getKVClient();
//		this.key = ByteSequence.from(connection.getKey());
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
//		CompletableFuture<GetResponse> getFuture = kvClient.get(key);
//		GetResponse response = null;
//		try {
//			response = getFuture.get();
//		} catch (InterruptedException | ExecutionException e) {
//			log.error("", e);
//		}
//		return response.getKvs();
		return AckMetaData.empty();
	}

	@Override
	public void putBytes(byte[] bytes) throws IOException {
//		ByteSequence value = ByteSequence.from("test_value".getBytes());
//		try {
//			kvClient.put(key, value).get();
//		} catch (InterruptedException | ExecutionException e) {
//			log.error("", e);
//		}
	}


	@Override
	public String toString() {
		return "EtcdBasedFile{" +
				"connection=" + connection.toConnectionUrl(null) +
				", key=" + connection.getKey() +
				'}';
	}
}
