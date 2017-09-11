package com.github.zzt93.syncer.config.filter;


/**
 * Created by zzt on 9/11/17.
 * <p>
 * <h3></h3>
 */
public interface Filter<E> {

    enum FilterRes {
        DENY,
        NEUTRAL,
        ACCEPT;
    }

    public abstract FilterRes decide(E e);

}
