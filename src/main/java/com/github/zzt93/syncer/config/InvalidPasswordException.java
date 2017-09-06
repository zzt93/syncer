package com.github.zzt93.syncer.config;

/**
 * @author zzt
 */
public class InvalidPasswordException extends IllegalArgumentException {
    public InvalidPasswordException() {
    }

    public InvalidPasswordException(String s) {
        super(s);
    }
}
