package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.Filter;
import com.github.zzt93.syncer.common.SyncData;
import java.util.List;

/**
 * @author zzt
 */
// TODO 18/1/10 use list or single? @see IfBodyAction
public interface ExprFilter extends Filter<List<SyncData>, Void> {

}
