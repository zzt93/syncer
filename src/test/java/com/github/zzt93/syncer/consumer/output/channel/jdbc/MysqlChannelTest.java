package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class MysqlChannelTest {

  @Test
  public void testGsonGeneric() throws Exception {
    TypeToken<SyncWrapper<String>> typeToken = new TypeToken<SyncWrapper<String>>() {
    };
    Assert.assertEquals(typeToken.getType().getTypeName(), "com.github.zzt93.syncer.common.data.SyncWrapper<java.lang.String>");
  }
}