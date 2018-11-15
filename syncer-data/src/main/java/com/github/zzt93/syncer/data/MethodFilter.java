package com.github.zzt93.syncer.data;


import java.util.List;

/**
 * @author zzt
 */
public interface MethodFilter {

  void filter(List<SyncData> list);

}
