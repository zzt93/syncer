package com.github.zzt93.syncer.filter;


/**
 * Created by zzt on 9/11/17.
 * <p>
 * <h3></h3>
 */
public interface Filter<E> {

    E decide(E e);

}
