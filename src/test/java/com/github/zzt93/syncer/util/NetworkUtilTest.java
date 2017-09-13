package com.github.zzt93.syncer.util;

import static org.junit.Assert.assertEquals;

import com.github.zzt93.syncer.common.util.NetworkUtil;
import org.junit.Test;

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