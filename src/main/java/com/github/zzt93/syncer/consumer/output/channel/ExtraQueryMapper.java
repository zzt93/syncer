package com.github.zzt93.syncer.consumer.output.channel;

import com.github.zzt93.syncer.common.data.InsertByQuery;
import com.github.zzt93.syncer.consumer.output.mapper.Mapper;
import java.util.Map;

/**
 * @author zzt
 */
public interface ExtraQueryMapper extends Mapper<InsertByQuery, Map<String, Object>> {

}
