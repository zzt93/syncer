package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.config.ConsumerOutputConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.util.Map;


@Getter
@Setter
@ToString
@ConsumerOutputConfig("output.hBase.connection")
public class HBaseConnection extends ClusterConnection {

  @ConsumerOutputConfig
  private int zkClientPort = HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT;
  @ConsumerOutputConfig
  private String zkQuorum;
  @ConsumerOutputConfig
  private Map<String, String> properties;

  public Configuration conf() {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    configuration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
    return configuration;
  }

  @Override
  public boolean valid() {
    return getZkClientPort() > 0 && getZkClientPort() < 65535 && !StringUtils.isEmpty(getZkQuorum());
  }

  @Override
  public String connectionIdentifier() {
    return zkQuorum + ":" + zkClientPort;
  }
}


