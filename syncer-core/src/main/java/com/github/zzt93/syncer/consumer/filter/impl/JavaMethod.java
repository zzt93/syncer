package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JavaMethod {

  private static final Logger logger = LoggerFactory.getLogger(JavaMethod.class);

  public static SyncFilter build(String sourcePath) {
    Path path = Paths.get(sourcePath);
    String className = FilenameUtils.removeExtension(path.getFileName().toString());

    compile(sourcePath);

    Class<?> cls;
    try {
      URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{path.getParent().toUri().toURL()}, JavaMethod.class.getClassLoader());
      cls = Class.forName(className, true, classLoader);
      return (SyncFilter) cls.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | MalformedURLException e) {
      ShutDownCenter.initShutDown(e);
      return null;
    }
  }

  private static void compile(String path) {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fm = compiler.getStandardFileManager(diagnostic -> logger.error("{}, {}", diagnostic.getLineNumber(), diagnostic.getSource().toUri()), null, null);
    try {
      fm.setLocation(StandardLocation.CLASS_PATH, Lists.newArrayList(new File(System.getProperty("java.class.path"))));
    } catch (IOException e) {
      logger.error("Fail to set location for compiler file manager", e);
    }
    if (compiler.run(null, null, null, path) != 0) {
      ShutDownCenter.initShutDown(new InvalidConfigException());
    }
  }

}
