package com.github.zzt93.syncer.connect;

/**
 * @author zzt
 */
public abstract class NamedJob implements Runnable {

    private final String name;

    public NamedJob(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
