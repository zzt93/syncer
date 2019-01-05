package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Collections;

public class JavaMethodTest {

  private static SyncData data = new SyncData("asdf", 1, "test", "test", "id", 1L, Collections.emptyMap(), EventType.UPDATE_ROWS);

  @Test
  public void build() {
    SyncFilter searcher = JavaMethod.build("searcher", new SyncerFilterMeta(), "    public void filter(List<SyncData> list) {\n" +
        "      for (SyncData d : list) {\n" +
        "        assert d.getEventId().equals(\"asdf\");\n" +
        "      }\n" +
        "    }\n");
    searcher.filter(Lists.newArrayList(data));
  }
}