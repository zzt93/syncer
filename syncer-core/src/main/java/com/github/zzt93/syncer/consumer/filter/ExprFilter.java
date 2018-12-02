package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.data.SyncData;

import java.util.List;

/**
 * This the abstraction of `filter` in pipeline. It can have side effect on `SyncData`, create more `SyncData`
 * or discard some `SyncData`.
 * <hr>
 * Its input use list to handle create/dup/remove and avoid multiple lists creation overhead.
 * <hr>
 *
 * @see com.github.zzt93.syncer.consumer.filter.impl.Create
 * @see com.github.zzt93.syncer.consumer.filter.impl.Dup
 * @author zzt
 */
public interface ExprFilter {

  void filter(List<SyncData> e);

}
