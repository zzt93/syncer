package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaMethod {

  private static final Logger logger = LoggerFactory.getLogger(JavaMethod.class);

  public static SyncFilter build(String consumerId, SyncerFilterMeta filterMeta, String method) {
    String source =
        // User write config using com.github.zzt93.syncer.data,
        // syncer run config using com.github.zzt93.syncer.common.data
        "import com.github.zzt93.syncer.common.data.*;\n" +
        "import com.github.zzt93.syncer.data.util.*;\n" +
        "\n" +
        "import java.util.*;\n" +
        "\n" +
        "import org.slf4j.Logger;\n" +
        "import org.slf4j.LoggerFactory;\n" +
        "\n" +
        "public class MethodFilterTemplate implements SyncFilter<SyncData> {\n" +
        "\n" +
        "  private final Logger logger = LoggerFactory.getLogger(MethodFilter.class);\n" +
        "\n" +
        addNewline(method) +
        "\n" +
        "}\n";

    String className = "Filter" + consumerId;
    source = source.replaceFirst("MethodFilterTemplate", className);
    Path path = Paths.get(filterMeta.getSrc(), className + ".java");
    try {
      Files.write(path, source.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      logger.error("No permission", e);
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    compile(path.toString());
//    compiler.run(null, null, null, path.toString());

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

  private static void compile(String s) {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
//    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    StandardJavaFileManager fm = compiler.getStandardFileManager(null, null, null);
//    try {
//      fm.setLocation(StandardLocation.CLASS_PATH, Arrays.asList(new File(System.getProperty("java.class.path"))));
//    } catch (IOException e) {
//      logger.error("", e);
//    }
//    return compiler;
    List<String> optionList = new ArrayList<>();
    optionList.addAll(Arrays.asList("-classpath",System.getProperty("java.class.path")));
//    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    Iterable<? extends JavaFileObject> fileObjects = fm.getJavaFileObjectsFromStrings(Lists.newArrayList(s));
    JavaCompiler.CompilationTask task = compiler.getTask(null, null, null, optionList, null, fileObjects);
    task.call();
  }

  private static String addNewline(String method) {
    char[] cs = method.toCharArray();
    StringBuilder sb = new StringBuilder(method.length() + 50);
    boolean inQuote = false;
    for (char c : cs) {
      sb.append(c);
      switch (c) {
        case '"':
          inQuote = !inQuote;
          break;
        case ';':
        case '{':
          if (!inQuote) sb.append('\n');
          break;
      }
    }
    return sb.toString();
  }
}
