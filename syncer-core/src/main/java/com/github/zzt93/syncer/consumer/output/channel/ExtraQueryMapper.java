package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;

import java.util.Map;

/**
 * @author zzt
 */
public interface ExtraQueryMapper extends Mapper<ExtraQuery, Map<String, Object>> {

}
