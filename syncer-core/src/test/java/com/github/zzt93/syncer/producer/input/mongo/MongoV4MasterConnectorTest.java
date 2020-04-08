package com.github.zzt93.syncer.producer.input.mongo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.bson.*;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

import static com.github.zzt93.syncer.producer.input.mongo.MongoMasterConnector.ID;
import static com.github.zzt93.syncer.producer.input.mongo.MongoV4MasterConnector.getUpdatedFields;
import static org.junit.Assert.*;

/**
 * @author zzt
 */
public class MongoV4MasterConnectorTest {
  private static final Gson gson = new GsonBuilder().create();

  @Test
  public void int64Id() {
    //9007199254740993
    long v = ((long) Math.pow(2, 53)) + 1;
    assertNotEquals(v, (double)v);

    BsonDocument key = new BsonDocument(ID, new BsonInt64(v));
    Object o = gson.fromJson(key.toJson(JsonWriterSettings.builder()
        .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
        .build()), Map.class).get(ID);
    assertNotEquals(v, o);
  }

  @Test
  public void types() {
    long v = ((long) Math.pow(2, 53)) + 1;
    long v1 = ((long) Math.pow(2, 53)) + 3;
    long v2 = ((long) Math.pow(2, 53)) + 5;
    long v4 = System.currentTimeMillis();
    double v3 =  Math.pow(2, 53) + 5;
    String time = "time";
    String id = "id";
    String deep = "criteriaAuditRecords.0.auditRoleIds.0";
    String str = "str";
    String dou = "double";
    String date = "date";
    String bool = "bool";
    String obj = "obj";

    BsonDocument updateDoc = new BsonDocument(time, new BsonInt64(v));
    Map map = getUpdatedFields(null, updateDoc, true);
    assertEquals(1, map.size());
    assertEquals(v, map.get(time));

    updateDoc.append(id, new BsonInt64(v1));
    map = getUpdatedFields(null, updateDoc, true);
    assertEquals(2, map.size());
    assertEquals(v, map.get(time));
    assertEquals(v1, map.get(id));

    updateDoc.append(deep, new BsonInt64(v2));
    map = getUpdatedFields(null, updateDoc, true);
    assertEquals(3, map.size());
    assertEquals(v, map.get(time));
    assertEquals(v1, map.get(id));
    assertEquals(v2, map.get(deep));

    updateDoc.append(str, new BsonString(str));
    updateDoc.append(dou, new BsonDouble(v3));
    updateDoc.append(date, new BsonDateTime(v4));
    updateDoc.append(bool, new BsonBoolean(true));
    updateDoc.append(obj, new BsonDocument(str, new BsonString(str)).append(id, new BsonInt64(v1)));
    map = getUpdatedFields(null, updateDoc, true);
    assertEquals(8, map.size());
    assertEquals(v, map.get(time));
    assertEquals(v1, map.get(id));
    assertEquals(v2, map.get(deep));
    assertEquals(str, map.get(str));
    assertEquals(v3, map.get(dou));
    assertTrue(map.get(date) instanceof Date);
    assertEquals(v4, ((Date) map.get(date)).getTime());
    assertEquals(true, map.get(bool));
    assertTrue(map.get(obj) instanceof Map);
    assertEquals(v1, ((Map) map.get(obj)).get(id));
  }
}