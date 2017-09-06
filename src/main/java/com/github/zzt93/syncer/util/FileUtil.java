package com.github.zzt93.syncer.util;

import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author zzt
 */
public class FileUtil {

    public static String readAll(String resourceName) throws IOException {
        return FileCopyUtils.copyToString(new InputStreamReader(ClassLoader.getSystemResourceAsStream(resourceName)));
    }
}
