package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.data.BinlogDataId;
import com.github.zzt93.syncer.common.data.DataId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author zzt
 */
public class FileBasedMapTest {

  private static final Path PATH = Paths.get("src/test/resources/FileBasedMapTest");
  private FileBasedMap<DataId> map;

  private static BinlogDataId getFromString(String s) {
    return (BinlogDataId) DataId.fromString(s);
  }

  @Before
  public void setUp() throws Exception {
    map = new FileBasedMap<>(PATH);
  }

  @Test
  public void flush() throws Exception {
    BinlogDataId _115 = getFromString("mysql-bin.000115/1234360405/139/0");
    map.append(_115, 2);
    map.flush();
    BinlogDataId s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(s, _115);
    map.remove(_115, 1);


    BinlogDataId _116 = getFromString("mysql-bin.000116/1305/139/0");
    map.append(_116, 1);
    map.flush();
    s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(s, _115);
    map.remove(_115, 1);
    s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(s, _115);
    map.flush();
    s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(s, _116);
    map.remove(_116, 1);


    BinlogDataId _117 = getFromString("mysql-bin.000117/1305/13/0");
    map.append(_117, 1);
    map.flush();
    s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(s, _117);
  }

  @Test
  public void testStringCompare() throws IOException {
    BinlogDataId _2 = getFromString("mysql-bin.000003/801814/478003/2");
    map.append(_2, 1);
    BinlogDataId _3 = getFromString("mysql-bin.000003/801814/478003/3");
    map.append(_3, 1);
    map.flush();
    BinlogDataId s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(_2, s);
    map.remove(_3, 1);
    map.flush();
    s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(_2, s);
    BinlogDataId _10 = getFromString("mysql-bin.000003/1001814/478003/0");
    map.append(_10, 1);
    map.flush();
    s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(_2, s);
  }

  @Test
  public void testLastRemoved() throws IOException {
    BinlogDataId _115 = getFromString("mysql-bin.000115/1234360405/139/0");
    map.append(_115, 2);
    map.remove(_115, 2);
    map.flush();
    BinlogDataId s = getFromString(new String(FileBasedMap.readData(PATH)));
    Assert.assertEquals(s, _115);
  }

  @Test
  public void multiThreadTest() {

  }
}