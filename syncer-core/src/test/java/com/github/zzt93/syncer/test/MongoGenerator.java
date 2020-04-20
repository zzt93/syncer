package com.github.zzt93.syncer.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bson.*;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.github.zzt93.syncer.common.util.RandomDataUtil.*;
import static java.lang.String.*;

/**
 * @author zzt
 */
public class MongoGenerator {


  public static final Double UPDATE_RATE = 0.3;
  public static final Double DELETE_RATE = 0.1;
  public static final int _1s = 1000;
  public static final String _4_0 = "4.0";
  public static final String _3_2 = "3.2";
  private static Random r = new Random();
  private final JsonWriterSettings js = JsonWriterSettings.builder().outputMode(JsonMode.SHELL)
      .int32Converter((value, writer) -> writer.writeRaw(format("NumberInt(%d)", value))).build();

  private static String randomStr() {
    return random(2, 10, true);
  }

  public static <T> T randomNull(T t) {
    if (r.nextDouble() < UPDATE_RATE) {
      return t;
    }
    return null;
  }

  @Test
  public void generate() throws FileNotFoundException {
    int num = Integer.parseInt(System.getProperty("num"));
    String start = System.getProperty("start");
    String version = System.getProperty("env");
    if (version.equals("mongo")) {
      version = _3_2;
    } else {
      version = _4_0;
    }
    Integer idStart = null;
    if (start != null) {
      idStart = Integer.parseInt(start);
    }
    String fileName = System.getProperty("fileName");
    PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(fileName)));
    List<BsonDocument> res = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      res.add(NestedOut.random(version, idStart != null ? idStart + i : null));
    }
    out.print(res.stream().map(BsonDocument::toJson).collect(Collectors.joining(",", "[", "]")));
    out.flush();
    out.close();

    if (start == null) return;

    out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(fileName + ".js")));
    out.println("db = db.getSiblingDB('simple_0');");
    String con = "{_id: %d}";
    for (int i = 0; i < num; i++) {
      if (r.nextDouble() < UPDATE_RATE) {
        int id = idStart + i;
        out.println(String.format("db.simple_type.updateMany(%s,{$set :%s});", String.format(con, id), NestedOut.randomUpdate(version, id).toJson(js)));
      }
    }

    for (int i = 0; i < num; i++) {
      if (r.nextDouble() < DELETE_RATE) {
        int id = idStart + i;
        out.println(String.format("db.simple_type.deleteOne(%s);", String.format(con, id)));
      }
    }

    out.flush();
    out.close();
  }

  private interface Doc {
    BsonDocument toDoc();
  }

  private static class Simple implements Doc {
    private final Long id;
    private final Byte tinyint;
    private final Long bigint;
    private final byte[] bytes;
    private final String varchar;
    private final BigDecimal decimal;
    private final Double aDouble;
    private final Timestamp timestamp;
    private final String version;

    Simple(String version, Long id, Byte tinyint, Long bigint, byte[] aBytes, String varchar, BigDecimal decimal, Double aDouble, Timestamp timestamp) {
      this.version = version;
      this.id = id;
      this.tinyint = tinyint;
      this.bigint = bigint;
      bytes = aBytes;
      this.varchar = varchar;
      this.decimal = decimal;
      this.aDouble = aDouble;
      this.timestamp = timestamp;
    }

    private static Simple random(String version) {
      return new Simple(version,
          r.nextLong(), randomByte(), r.nextLong(), randomBytes(50), randomStr(),
          randomDecimal(), r.nextDouble() + r.nextInt(), randomTimestamp()
      );
    }

    public static Simple randomUpdate(String version) {
      return new Simple(
          version, randomNull(r.nextLong()), randomNull(randomByte()), randomNull(r.nextLong()), randomNull(randomBytes(50)), randomNull(randomStr()),
          randomNull(randomDecimal()), randomNull(r.nextDouble() + r.nextInt()), randomNull(randomTimestamp())
      );
    }

    @Override
    public BsonDocument toDoc() {
      BsonDocument res = new BsonDocument();
      if (id != null) {
        res.append("id", new BsonInt64(id));
      }
      if (tinyint != null) {
        res.append("tinyint", new BsonInt32(tinyint));
      }
      if (bigint != null) {
        res.append("bigint", new BsonInt64(bigint));
      }
      if (bytes != null) {
        res.append("bytes", new BsonBinary(bytes));
      }
      if (varchar != null) {
        res.append("varchar", new BsonString(varchar));
      }
      if (decimal != null && MongoVersion.from(version).onOrAfter(_3_4)) {
        res.append("decimal", new BsonDecimal128(new Decimal128(decimal)));
      }
      if (aDouble != null) {
        res.append("aDouble", new BsonDouble(aDouble));
      }
      if (timestamp != null) {
        long time = timestamp.getTime();
        BsonTimestamp value = new BsonTimestamp((int) (time / _1s), (int) (time % _1s));
        res.append("timestamp", value);
      }
      return res;
    }
  }

  private static MongoVersion _3_4 = MongoVersion.from("3.4");
  @Data
  @AllArgsConstructor
  private static class MongoVersion {
    private int main;
    private int sub;
    public static MongoVersion from(String version) {
      String[] split = version.split("\\.");
      return new MongoVersion(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
    }
    public boolean onOrAfter(MongoVersion version) {
      return main > version.main || (main == version.main && sub >= version.sub);
    }
  }
  private static class NestedIn implements Doc {
    private final Long id;
    private final Date time;
    private final String currency;
    private final String total;
    private final Integer quantity;
    private final Byte type;
    private final String name;
    private final String unit;

    NestedIn(Long id, Date time, String currency, String total, Integer quantity, Byte type, String name, String unit) {
      this.id = id;
      this.time = time;
      this.currency = currency;
      this.total = total;
      this.quantity = quantity;
      this.type = type;
      this.name = name;
      this.unit = unit;
    }

    private static NestedIn random() {
      return new NestedIn(
          r.nextLong(), randomDate(), randomStr(), randomDecimal().toString(),
          r.nextInt(Short.MAX_VALUE), randomByte(), randomStr(), randomStr()
      );
    }

    public static NestedIn randomUpdate() {
      return new NestedIn(
          randomNull(r.nextLong()), randomNull(randomDate()), randomNull(randomStr()), randomNull(randomDecimal().toString()),
          randomNull(r.nextInt(Short.MAX_VALUE)), randomNull(randomByte()), randomNull(randomStr()), randomNull(randomStr())
      );
    }

    @Override
    public BsonDocument toDoc() {
      BsonDocument res = new BsonDocument();
      if (id != null) {
        res.append("id", new BsonInt64(id));
      }
      if (time != null) {
        res.append("time", new BsonDateTime(time.getTime()));
      }
      if (currency != null) {
        res.append("currency", new BsonString(currency));
      }
      if (total != null) {
        res.append("total", new BsonString(total));
      }
      if (quantity != null) {
        res.append("quantity", new BsonInt32(quantity));
      }
      if (type != null) {
        res.append("type", new BsonInt32(type));
      }
      if (name != null) {
        res.append("name", new BsonString(name));
      }
      if (unit != null) {
        res.append("unit", new BsonString(unit));
      }
      return res;
    }
  }

  private static class NestedOut implements Doc {

    private final List<Simple> simples;
    private final NestedIn nestedIn;
    private Long _id;

    NestedOut(List<Simple> simples, NestedIn nestedIn) {
      this.simples = simples;
      this.nestedIn = nestedIn;
    }

    NestedOut(long id, List<Simple> simples, NestedIn nestedIn) {
      this._id = id;
      this.simples = simples;
      this.nestedIn = nestedIn;
    }

    private static BsonDocument random(String version, Integer id) {
      int i = r.nextInt(10);
      ArrayList<Simple> simples = new ArrayList<>();
      for (int c = 0; c < i; c++) {
        simples.add(Simple.random(version));
      }
      if (id == null) {
        return new NestedOut(simples, NestedIn.random()).toDoc();
      }
      return new NestedOut(id, simples, NestedIn.random()).toDoc();
    }

    private static BsonDocument randomUpdate(String version, long id) {
      int i = r.nextInt(10);
      ArrayList<Simple> simples = new ArrayList<>();
      for (int c = 0; c < i; c++) {
        if (r.nextDouble() < UPDATE_RATE) {
          simples.add(Simple.randomUpdate(version));
        }
      }
      return new NestedOut(id, simples, NestedIn.randomUpdate()).toDoc();
    }

    @Override
    public BsonDocument toDoc() {
      BsonDocument res = new BsonDocument()
          .append("simples", new BsonArray(simples.stream().map(Simple::toDoc).collect(Collectors.toList())))
          .append("nestedIn", nestedIn.toDoc());
      if (_id != null) {
        res.append("_id", new BsonInt64(_id));
      }
      return res;
    }
  }
}
