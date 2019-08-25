package com.github.zzt93.syncer.consumer.ack;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author zzt
 */
public class FileBasedMapTest {

  private static final Path PATH = Paths.get("src/test/resources/FileBasedMapTest");
  private FileBasedMap<String> map;

  @Before
  public void setUp() throws Exception {
    map = new FileBasedMap<>(PATH);
  }

  @Test
  public void flush() throws Exception {
    String _115 = "mysql-bin.000115/1234360405/139/0";
    map.append(_115, 2);
    map.flush();
    String s = new String(FileBasedMap.readData(PATH));
    Assert.assertEquals(s, _115);
    map.remove(_115, 1);


    String _116 = "mysql-bin.000116/1305/139/0";
    map.append(_116, 1);
    map.flush();
    s = new String(FileBasedMap.readData(PATH));
    Assert.assertEquals(s, _115);
    map.remove(_115, 1);
    s = new String(FileBasedMap.readData(PATH));
    Assert.assertEquals(s, _115);
    map.flush();
    s = new String(FileBasedMap.readData(PATH));
    Assert.assertEquals(s, _116);
    map.remove(_116, 1);


    String _117 = "mysql-bin.000117/1305/13/0";
    map.append(_117, 1);
    map.flush();
    s = new String(FileBasedMap.readData(PATH));
    Assert.assertEquals(s, _117);
  }

  @Test
  public void testLastRemoved() {

  }

  @Test
  public void multiThreadTest() {

  }
}