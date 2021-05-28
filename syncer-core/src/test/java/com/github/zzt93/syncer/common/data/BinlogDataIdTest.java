package com.github.zzt93.syncer.common.data;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author zzt
 */
public class BinlogDataIdTest {

  @Test
  public void eventId() {
  }

  @Test
  public void dataId() {
  }

  @Test
  public void equals1() {
  }

  @Test
  public void compareTo() {
    BinlogDataId _0 = new BinlogDataId("mysql-bin.000117", 4, 40).setOrdinal(0).setCopy(null);
    BinlogDataId _00 = new BinlogDataId("mysql-bin.000117", 4, 40).setOrdinal(0).setCopy(0);
    BinlogDataId _1 = new BinlogDataId("mysql-bin.000117", 4, 40).setOrdinal(1).setCopy(null);
    BinlogDataId _14 = new BinlogDataId("mysql-bin.000117", 14, 40).setOrdinal(0).setCopy(null);
    BinlogDataId _118 = new BinlogDataId("mysql-bin.000118", 4, 40).setOrdinal(1).setCopy(null);
    Assert.assertEquals(-1, _0.compareTo(_00));
    Assert.assertEquals(1, _00.compareTo(_0));
    Assert.assertEquals(-1, _0.compareTo(_1));
    Assert.assertEquals(-1, _00.compareTo(_1));
    Assert.assertEquals(-1, _00.compareTo(_14));
    Assert.assertEquals(-1, _1.compareTo(_14));
    Assert.assertEquals(-1, _0.compareTo(_118));
    Assert.assertEquals(-1, _1.compareTo(_118));
    Assert.assertEquals(-1, _00.compareTo(_118));
    Assert.assertEquals(-1, _14.compareTo(_118));

    Assert.assertEquals(0, _0.compareTo(_0));
    Assert.assertEquals(0, _00.compareTo(_00));

    Assert.assertEquals(-1, ColdStartDataId.BINLOG_COLD.compareTo(_0));
  }

}
