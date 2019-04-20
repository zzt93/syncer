package com.github.zzt93.syncer.common.data;

/**
 * @author zzt
 */
public interface Mapper<I, O> {

  O map(I i);
}
