package com.github.zzt93.syncer.consumer;

import com.github.zzt93.syncer.config.common.EtcdConnection;
import com.github.zzt93.syncer.config.consumer.ConsumerConfig;
import com.github.zzt93.syncer.config.consumer.filter.FilterConfig;
import com.github.zzt93.syncer.config.consumer.input.PipelineInput;
import com.github.zzt93.syncer.config.consumer.output.PipelineOutput;
import com.github.zzt93.syncer.config.syncer.SyncerAck;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.config.syncer.SyncerInput;
import com.github.zzt93.syncer.config.syncer.SyncerOutput;
import com.github.zzt93.syncer.stat.SyncerInfo;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class ConsumerInitContext {

	private SyncerInfo syncerInfo;
	private SyncerConfig syncerConfig;
	private ConsumerConfig consumerConfig;

	public EtcdConnection getEtcd() {
		return syncerConfig.getEtcd().setInstanceId(syncerInfo.getInstanceId()).setConsumerId(getConsumerId());
	}

	public boolean hasEtcd() {
		return syncerConfig.hasEtcd();
	}

	public int getPort() {
		return syncerConfig.getPort();
	}

	public SyncerAck getAck() {
		return syncerConfig.getAck();
	}

	public PipelineInput getInput() {
		return consumerConfig.getInput();
	}


	public PipelineOutput getOutput() {
		return consumerConfig.getOutput();
	}

	public List<FilterConfig> getFilter() {
		return consumerConfig.getFilter();
	}

	public SyncerInput getSyncerInput() {
		return syncerConfig.getInput();
	}

	public SyncerFilter getSyncerFilter() {
		return syncerConfig.getFilter();
	}

	public int outputSize() {
		return consumerConfig.outputSize();
	}

	public SyncerOutput getSyncerOutput() {
		return syncerConfig.getOutput();
	}

	public String getConsumerId() {
		return consumerConfig.getConsumerId();
	}


}
