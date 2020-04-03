package com.github.zzt93.syncer.producer.input.mongo;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;

import java.util.Map;

import static com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector.ID;
import static com.github.zzt93.syncer.producer.input.mongo.MongoV4MasterConnector.getId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author zzt
 */
public class MongoV4MasterConnectorTest {

  @Test
  public void int64() {
    //9007199254740993
    long v = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(v, (double)v);

    BsonDocument key = new BsonDocument("_id", new BsonInt64(v));
    Object o = MongoV4MasterConnector.gson.fromJson(key.toJson(JsonWriterSettings.builder()
        .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
        .build()), Map.class).get(ID);
    assertNotEquals(v, o);

    Object now = getId(key);
    assertEquals(v, now);
  }
}