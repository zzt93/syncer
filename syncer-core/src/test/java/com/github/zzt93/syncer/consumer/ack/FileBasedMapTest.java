package com.github.zzt93.syncer.consumer.ack;

import com.github.zzt93.syncer.common.data.BinlogDataId;
import com.github.zzt93.syncer.common.data.DataId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zzt
 */
public class FileBasedMapTest {

  private static final Path PATH = Paths.get("src/test/resources/FileBasedMapTest");
  private static final Path EMPTY = Paths.get("src/test/resources/FileBasedMapTest_Empty");
  private FileBasedMap<DataId> notFirstTime;
  private FileBasedMap<DataId> firstTime;

  private static BinlogDataId getFromString(String s) {
    return (BinlogDataId) DataId.fromString(s);
  }

  @Before
  public void setUp() throws Exception {
    notFirstTime = new FileBasedMap<>(PATH);
    Files.write(EMPTY, new byte[0], StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    firstTime = new FileBasedMap<>(EMPTY);
  }

  @Test
  public void testFlush() throws Exception {
    BinlogDataId _115 = getFromString("mysql-bin.000115/1234360405/139/0");
    notFirstTime.append(_115, 2);
    notFirstTime.flush();
    BinlogDataId s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
    notFirstTime.remove(_115, 1);


    BinlogDataId _116 = getFromString("mysql-bin.000116/1305/139/0");
    notFirstTime.append(_116, 1);
    notFirstTime.flush();
    s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
    notFirstTime.remove(_115, 1);
    s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
    notFirstTime.flush();
    s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(s, _116);
    notFirstTime.remove(_116, 1);


    BinlogDataId _117 = getFromString("mysql-bin.000117/1305/13/0");
    notFirstTime.append(_117, 1);
    notFirstTime.flush();
    s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(s, _117);
  }

  @Test
  public void testStringCompare() throws IOException {
    BinlogDataId _2 = getFromString("mysql-bin.000003/801814/478003/2");
    notFirstTime.append(_2, 1);
    BinlogDataId _3 = getFromString("mysql-bin.000003/801814/478003/3");
    notFirstTime.append(_3, 1);
    notFirstTime.flush();
    BinlogDataId s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(_2, s);
    notFirstTime.remove(_3, 1);
    notFirstTime.flush();
    s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(_2, s);
    BinlogDataId _10 = getFromString("mysql-bin.000003/1001814/478003/0");
    notFirstTime.append(_10, 1);
    notFirstTime.flush();
    s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(_2, s);
  }

  @Test
  public void testLastRemoved() throws IOException {
    BinlogDataId _115 = getFromString("mysql-bin.000115/1234360405/139/0");
    notFirstTime.append(_115, 2);
    notFirstTime.remove(_115, 2);
    notFirstTime.flush();
    BinlogDataId s = getFromString(notFirstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
  }

  @Test
  public void testFlushFirstTime() throws Exception {
    BinlogDataId _115 = getFromString("mysql-bin.000115/1234360405/139/0");
    firstTime.append(_115, 2);
    firstTime.flush();
    BinlogDataId s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
    firstTime.remove(_115, 1);


    BinlogDataId _116 = getFromString("mysql-bin.000116/1305/139/0");
    firstTime.append(_116, 1);
    firstTime.flush();
    s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
    firstTime.remove(_115, 1);
    s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
    firstTime.flush();
    s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(s, _116);
    firstTime.remove(_116, 1);


    BinlogDataId _117 = getFromString("mysql-bin.000117/1305/13/0");
    firstTime.append(_117, 1);
    firstTime.flush();
    s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(s, _117);
  }

  @Test
  public void testStringCompareFirstTime() throws IOException {
    BinlogDataId _2 = getFromString("mysql-bin.000003/801814/478003/2");
    firstTime.append(_2, 1);
    BinlogDataId _3 = getFromString("mysql-bin.000003/801814/478003/3");
    firstTime.append(_3, 1);
    firstTime.flush();
    BinlogDataId s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(_2, s);
    firstTime.remove(_3, 1);
    firstTime.flush();
    s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(_2, s);
    BinlogDataId _10 = getFromString("mysql-bin.000003/1001814/478003/0");
    firstTime.append(_10, 1);
    firstTime.flush();
    s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(_2, s);
  }

  @Test
  public void testLastRemovedFirstTime() throws IOException {
    BinlogDataId _115 = getFromString("mysql-bin.000115/1234360405/139/0");
    firstTime.append(_115, 2);
    firstTime.remove(_115, 2);
    firstTime.flush();
    BinlogDataId s = getFromString(firstTime.readData().toDataStr());
    Assert.assertEquals(s, _115);
  }

  @Test
  public void multiThreadTest() throws IOException, InterruptedException {
    ExecutorService two = Executors.newFixedThreadPool(2);
    int times = 1000;
    BinlogDataId _003 = getFromString("mysql-bin.000003/801814/478003/2");
    BinlogDataId _117 = getFromString("mysql-bin.000117/1305/13/0");
    firstTime.append(_117, 1);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch end = new CountDownLatch(2);
    two.submit(() -> {
      try {
        start.await();
      } catch (InterruptedException ignored) {
      }
      for (int i = 0; i < times; i++) {
        firstTime.append(_003, 1);
        firstTime.flush();
      }
      end.countDown();
    });
    two.submit(() -> {
      start.countDown();
      for (int i = 0; i < times; i++) {
        try {
          firstTime.remove(_003, 1);
        } catch (Exception ignored) {
        }
        firstTime.flush();
      }
      end.countDown();
    });

    end.await();

    BinlogDataId s = getFromString(firstTime.readData().toDataStr());
    Assert.assertTrue(s.equals(_003) || s.equals(_117));
  }

  @Test
  public void testToString() {
    Assert.assertEquals("FileBasedMap{path=LocalMetaFile{path=src/test/resources/FileBasedMapTest}, lastRemoved=null, map={}}", notFirstTime.toString());
  }
}