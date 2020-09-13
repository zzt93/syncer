package com.github.zzt93.syncer.config.consumer.input;


import com.github.zzt93.syncer.config.common.MasterSource;
import lombok.Getter;

import java.util.Set;

/**
 * @author zzt
 */
@Getter
public class PipelineInput {

	private final Set<MasterSource> masterSet;

	public PipelineInput(Set<MasterSource> masterSet) {
		this.masterSet = masterSet;
	}
}
