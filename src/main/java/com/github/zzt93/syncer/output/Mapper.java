package com.github.zzt93.syncer.output;

/**
 * @author zzt
 */
public interface Mapper<I, O> {

  O map(I i);
}
