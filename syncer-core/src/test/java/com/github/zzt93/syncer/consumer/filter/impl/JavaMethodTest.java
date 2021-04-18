package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.data.SimpleEventType;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.github.zzt93.syncer.producer.dispatch.mysql.event.NamedFullRow;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

public class JavaMethodTest {

  private static SyncData data = new SyncData(new MongoDataId(123, 1), SimpleEventType.UPDATE, "test", "test", "id", 1L, new NamedFullRow(Maps.newHashMap()));

  @Test
  public void build() {
    SyncFilter searcher = JavaMethod.build("src/test/java/MethodTestBuild.java");
    searcher.filter(Lists.newArrayList(data));
  }

  @Test
  public void testLoadTwice() {
    SyncFilter searcher = JavaMethod.build("src/test/java/MethodTestDateTest.java");
    searcher.filter(Lists.newArrayList(data));
    searcher = JavaMethod.build("src/test/java/MethodTestDateTest.java");
    searcher.filter(Lists.newArrayList(data));
  }

}