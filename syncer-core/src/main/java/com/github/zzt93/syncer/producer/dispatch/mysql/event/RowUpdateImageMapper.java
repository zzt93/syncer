package com.github.zzt93.syncer.producer.dispatch.mysql.event;

import com.github.zzt93.syncer.common.data.Mapper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * @author zzt
 */
public interface RowUpdateImageMapper extends Mapper<Entry<Serializable[], Serializable[]>, HashMap<Integer, Object>> {


}
