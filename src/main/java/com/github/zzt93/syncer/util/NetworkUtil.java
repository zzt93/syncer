package com.github.zzt93.syncer.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zzt
 */
public class NetworkUtil {

    public static String toIp(String host) throws UnknownHostException {
        InetAddress address = InetAddress.getByName(host);
        return address.getHostAddress();
    }
}
