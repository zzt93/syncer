package com.github.zzt93.syncer.producer.input.mongo;

import org.bson.BsonTimestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author zzt
 */
public class DocTimestampTest {

  @Test
  public void compareTo() {
    DocTimestamp b_e = DocTimestamp.earliest;
    DocTimestamp b_l = DocTimestamp.latest;
    DocTimestamp b__l = DocTimestamp.latest;
    DocTimestamp b0 = new DocTimestamp( new BsonTimestamp(0));
    DocTimestamp b1 = new DocTimestamp( new BsonTimestamp(1));
    DocTimestamp b2 = new DocTimestamp( new BsonTimestamp(2));
    DocTimestamp bc = new DocTimestamp( new BsonTimestamp(System.currentTimeMillis()));

    assertEquals(b_e.compareTo(b0), -1);
    assertEquals(b_l.compareTo(bc), 1);
    assertEquals(b_e.compareTo(b_l), -1);
    assertEquals(b_l.compareTo(b__l), 0);
    assertEquals(b0.compareTo(b2), -1);
    assertEquals(b1.compareTo(b2), -1);
    assertEquals(b2.compareTo(b0), 1);
    assertEquals(b2.compareTo(bc), -1);
  }
}