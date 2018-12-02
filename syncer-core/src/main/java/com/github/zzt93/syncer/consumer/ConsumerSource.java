package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncInitMeta;
import com.github.zzt93.syncer.config.consumer.common.Connection;
import com.github.zzt93.syncer.config.consumer.input.Repo;

import java.util.Set;

/**
 * Class used to register consumer to producer
 *
 * @see com.github.zzt93.syncer.producer.register.LocalConsumerRegistry#register(Connection, ConsumerSource)
 * @author zzt
 */
public interface ConsumerSource extends Hashable {

  Connection getRemoteConnection();

  SyncInitMeta getSyncInitMeta();

  Set<Repo> getRepos();

  String clientId();

  boolean input(SyncData data);

  boolean input(SyncData[] data);

  String toString();

  boolean sent(SyncData data);

}
