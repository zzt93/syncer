package com.github.zzt93.syncer.consumer.ack;

import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

/**
 * @author zzt
 */
public class FileBasedMapTest {

  private FileBasedMap<String> map;

  @Before
  public void setUp() throws Exception {
    map = new FileBasedMap<>(Paths.get("src/test/resources/FileBasedMapTest"));
  }

  @Test
  public void flush() throws Exception {
    map.append("mysql-bin.000115/1234360405/139/u", 2);
    map.flush();
    map.remove("mysql-bin.000115/1234360405/139/u", 2);
    map.append("mysql-bin.000116/1305/139/w", 1);
    map.flush();
    map.remove("mysql-bin.000116/1305/139/w", 1);
    map.append("mysql-bin.000116/1305/13/w", 1);
    map.flush();
  }

}