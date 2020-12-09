package com.github.zzt93.syncer.common.util;

import com.github.zzt93.syncer.producer.input.mysql.AlterMeta;
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
    test = SQLHelper.alterMeta("", "alter table test.xx add yy int null after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table xx drop column yy");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta("", "alter table test.xx drop column yy");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta(TEST, "alter table xx modify column yy int after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta("", "alter table test.xx modify column yy int after zz");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta("", "alter table `test`.`xx` modify column `yy` int after `zz`");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());
    test = SQLHelper.alterMeta("", "alter table `test`\n.`xx`\n modify column `yy` int after `zz`");
    assertNotNull(test);
    assertEquals("", TEST, test.getSchema());
    assertEquals("", XX, test.getTable());

    test = SQLHelper.alterMeta(TEST, "alter table xx add yy int null");
    assertNull(test);
    test = SQLHelper.alterMeta("", "alter table test.xx add yy int null");
    assertNull(test);
    test = SQLHelper.alterMeta("test", "/* comment */ alter table xx alter column credit_total drop default'");
    assertNull(test);
  }
}