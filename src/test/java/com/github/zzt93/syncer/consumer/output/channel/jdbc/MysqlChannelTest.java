package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.google.gson.reflect.TypeToken;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class MysqlChannelTest {

  @Test
  public void testGsonGeneric() throws Exception {
    TypeToken<List<String>> typeToken = new TypeToken<List<String>>() {
    };
    Assert.assertEquals(typeToken.getType().getTypeName(), "java.util.List<java.lang.String>");
    TypeToken<List<List<Integer>>> list = new TypeToken<List<List<Integer>>>() {
    };
    Assert.assertEquals(list.getType().getTypeName(), "java.util.List<java.util.List<java.lang.Integer>>");
  }
}