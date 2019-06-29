package com.github.zzt93.syncer.producer.input.mysql.connect;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author zzt
 */
public class BinlogInfoTest {

  @Test
  public void compareTo() throws Exception {
    BinlogInfo b0 = BinlogInfo.earliest;
    BinlogInfo b_ = BinlogInfo.latest;
    BinlogInfo b1 = new BinlogInfo("mysql-bin.00001", 0);
    BinlogInfo b2 = new BinlogInfo("mysql-bin.00002", 0);
    BinlogInfo b2_1 = new BinlogInfo("mysql-bin.00002", 1);
    BinlogInfo b2_2 = new BinlogInfo("mysql-bin.00002", 2);
    BinlogInfo b20_2 = new BinlogInfo("mysql-bin.00020", 2);
    BinlogInfo b200_2 = new BinlogInfo("mysql-bin.200", 2);
    BinlogInfo b1200_2 = new BinlogInfo("mysql-bin.1200", 2);

    assertEquals(b0.compareTo(b1), -1);
    assertEquals(b1.compareTo(b2), -1);
    assertEquals(b2.compareTo(b2_1), -1);
    assertEquals(b2_1.compareTo(b2_2), -1);
    assertEquals(b2_2.compareTo(b20_2), -1);
    assertEquals(b20_2.compareTo(b200_2), -1);
    assertEquals(b200_2.compareTo(b1200_2), -1);
  }

  @Test(expected = InvalidBinlogException.class)
  public void filenameCheck() throws Exception {
    BinlogInfo.checkFilename("mysql-bin1");
  }


  @Test(expected = InvalidBinlogException.class)
  public void filenameCheck2() throws Exception {
    BinlogInfo.checkFilename("mysql-bin.00001bak");
  }


}