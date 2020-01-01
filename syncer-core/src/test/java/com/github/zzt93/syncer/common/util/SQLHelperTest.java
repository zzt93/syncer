package com.github.zzt93.syncer.common.util;

import com.github.zzt93.syncer.producer.input.mysql.AlterMeta;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SQLHelperTest {

  private static final String TEST = "test";
  public static final String XX = "xx";

  @Test
  public void alterMeta() {
    AlterMeta test = SQLHelper.alterMeta(TEST, "alter table xx add yy int null after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table test.xx add yy int null after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table xx drop column yy");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table test.xx drop column yy");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table xx modify column yy int after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table test.xx modify column yy int after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table xx add yy int null");
    assertNull(test);
    test = SQLHelper.alterMeta(TEST, "alter table test.xx add yy int null");
    assertNull(test);
  }
}