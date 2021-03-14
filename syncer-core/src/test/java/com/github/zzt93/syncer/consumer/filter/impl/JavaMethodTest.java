package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class JavaMethodTest {

  private static SyncData data = new SyncData(new MongoDataId(123, 1), SimpleEventType.UPDATE, "test", "test", "id", 1L, new NamedFullRow(Maps.newHashMap()));

  @Test
  public void build() {
    SyncFilter searcher = JavaMethod.build("searcher", new SyncerFilterMeta(), "    public void filter(List<SyncData> list) {" +
        "      for (SyncData d : list) {" +
        "        assert d.getEventId().equals(\"123/1\");" +
        "      }" +
        "    }");
    searcher.filter(Lists.newArrayList(data));
  }

  @Test
  public void getClassSource() {
    String classSource = JavaMethod.getClassSource("  public void filter(List<SyncData> list) {" +
        "    Function<Object, String> function = Object::toString;" +
        "    char c = ':';" +
        "        String expr = \"123\"" +
        "        switch (expr) {" +
        "          case SyncDataTypeUtil.ROW_ALL:" +
        "          case SyncDataTypeUtil.EXTRA_ALL:" +
        "          case SyncDataTypeUtil.ROW_FLATTEN:" +
        "          case SyncDataTypeUtil.EXTRA_FLATTEN:" +
        "            res.put(key, expr);" +
        "            break;" +
        "          default:" +
        "            Expression expression = parser.parseExpression(expr);" +
        "            res.put(key, expression);" +
        "            break;" +
        "        }" +
        "      for (SyncData d : list) {" +
        "        assert d.getEventId().equals(\"123/1\");" +
        "      }" +
        "  }");
    Assert.assertEquals("import com.github.zzt93.syncer.data.*;\n" +
        "import com.github.zzt93.syncer.data.es.*;\n" +
        "import com.github.zzt93.syncer.data.util.*;\n" +
        "import java.util.*;\n" +
        "import java.util.stream.*;\n" +
        "import java.math.BigDecimal;\n" +
        "import java.sql.*;\n" +
        "import org.slf4j.Logger;\n" +
        "import org.slf4j.LoggerFactory;\n" +
        "\n" +
        "public class MethodFilterTemplate implements SyncFilter<SyncData> {\n" +
        "\n" +
        "  private final Logger logger = LoggerFactory.getLogger(getClass());\n" +
        "\n" +
        "  public void filter(List<SyncData> list) {\n" +
        "    Function<Object, String> function = Object::toString;\n" +
        "    char c = ':';\n" +
        "        String expr = \"123\"        switch (expr) {\n" +
        "          case SyncDataTypeUtil.ROW_ALL:          case SyncDataTypeUtil.EXTRA_ALL:          case SyncDataTypeUtil.ROW_FLATTEN:          case SyncDataTypeUtil.EXTRA_FLATTEN:            res.put(key, expr);\n" +
        "            break;\n" +
        "          default:            Expression expression = parser.parseExpression(expr);\n" +
        "            res.put(key, expression);\n" +
        "            break;\n" +
        "        }\n" +
        "      for (SyncData d : list) {\n" +
        "        assert d.getEventId().equals(\"123/1\");\n" +
        "      }\n" +
        "  }\n" +
        "\n" +
        "}\n", classSource);
  }


}