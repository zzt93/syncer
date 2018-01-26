package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.data.SyncData;
import java.util.List;

/**
 * @author zzt
 * have to use list to handle clone/dup
 */
public interface ExprFilter extends Filter<List<SyncData>, Void> {

}
