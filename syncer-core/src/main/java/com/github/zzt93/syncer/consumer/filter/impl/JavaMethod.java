package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.data.util.SyncFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
    compiler.run(null, null, null, path.toString());

    Class<?> cls;
    try {
      URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{path.getParent().toUri().toURL()}, JavaMethod.class.getClassLoader());
      cls = Class.forName(className, true, classLoader);
      return (SyncFilter) cls.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | MalformedURLException e) {
      logger.error("Syncer bug", e);
      return null;
    }
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
