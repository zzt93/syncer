package com.github.zzt93.syncer.consumer.output.mapper;

/**
 * @author zzt
 */
public interface Mapper<I, O> {

  O map(I i);
}
