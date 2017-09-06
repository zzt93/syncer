package com.github.zzt93.syncer.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author zzt
 */
public class NetworkUtilTest {
    @Test
    public void toIp() throws Exception {
        String host = "192.168.1.100";
        String s = NetworkUtil.toIp(host);
        assertEquals(s, host);
    }

}