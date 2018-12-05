package com.github.zzt93.syncer.data;


import java.util.List;

/**
 * This the abstraction of `filter` in pipeline. It can have side effect on `SyncData`, create more `SyncData`
 * or discard some `SyncData`.
 * <hr>
 * Its input use list to handle create/dup/remove and avoid multiple lists creation overhead.
 * <hr>
 *
 * @author zzt
 */
public interface SyncFilter<T extends SyncData> {

  void filter(List<T> e);

}
