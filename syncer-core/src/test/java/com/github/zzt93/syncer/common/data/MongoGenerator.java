package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.common.util.RandomDataUtil;
import org.bson.*;
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

/**
 * @author zzt
 */
public class MongoGenerator {


  private static Random r = new Random();

  @Test
  public void generate() throws FileNotFoundException {
    int num = Integer.parseInt(System.getProperty("num"));
    String start = System.getProperty("start");
    Integer idStart = null;
    if (start != null) {
      idStart = Integer.parseInt(start);
    }
    String fileName = System.getProperty("fileName");
    PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(fileName)));
    List<BsonDocument> res = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      res.add(NestedOut.random(idStart != null ? idStart + i : null));
    }
    out.print(res.stream().map(BsonDocument::toJson).collect(Collectors.joining(",", "[", "]")));
    out.flush();
    out.close();
  }

  private interface Doc {
    BsonDocument toDoc();
  }

  private static String randomStr() {
    return RandomDataUtil.random(2, 10, true);
  }

  private static class Simple implements Doc{
    private final long id;
    private final byte tinyint;
    private final long bigint;
    private final byte[] bytes;
    private final String varchar;
    private final BigDecimal decimal;
    private final double aDouble;
    private final Timestamp timestamp;

    Simple(long id, byte tinyint, long bigint, byte[] aBytes, String varchar, BigDecimal decimal, double aDouble, Timestamp timestamp) {
      this.id = id;
      this.tinyint = tinyint;
      this.bigint = bigint;
      bytes = aBytes;
      this.varchar = varchar;
      this.decimal = decimal;
      this.aDouble = aDouble;
      this.timestamp = timestamp;
    }

    private static Simple random() {
      return new Simple(
          r.nextLong(), RandomDataUtil.randomByte(), r.nextLong(), RandomDataUtil.randomBytes(50), randomStr(),
          RandomDataUtil.randomDecimal(), r.nextDouble() + r.nextInt(), RandomDataUtil.randomTimestamp()
      );
    }

    @Override
    public BsonDocument toDoc() {
      return new BsonDocument()
          .append("id", new BsonInt64(id))
          .append("tinyint", new BsonInt32(tinyint))
          .append("bigint", new BsonInt64(bigint))
          .append("bytes", new BsonBinary(bytes))
          .append("varchar", new BsonString(varchar))
          .append("decimal", new BsonDecimal128(new Decimal128(decimal)))
          .append("aDouble", new BsonDouble(aDouble))
          .append("timestamp", new BsonTimestamp(timestamp.getTime()))
          ;
    }
  }

  private static class NestedIn implements Doc {
    private final long id;
    private final Date time;
    private final String currency;
    private final String total;
    private final int quantity;
    private final byte type;
    private final String name;
    private final String unit;

    NestedIn(long id, Date time, String currency, String total, int quantity, byte type, String name, String unit) {
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
        r.nextLong(), RandomDataUtil.randomDate(), randomStr(), RandomDataUtil.randomDecimal().toString(),
        r.nextInt(Short.MAX_VALUE), RandomDataUtil.randomByte(), randomStr(), randomStr()
      );
    }

    @Override
    public BsonDocument toDoc() {
      return new BsonDocument()
          .append("id", new BsonInt64(id))
          .append("time", new BsonDateTime(time.getTime()))
          .append("currency", new BsonString(currency))
          .append("total", new BsonString(total))
          .append("quantity", new BsonInt32(quantity))
          .append("type", new BsonInt32(type))
          .append("name", new BsonString(name))
          .append("unit", new BsonString(unit))
          ;
    }
  }

  private static class NestedOut implements Doc {

    private Long _id;
    private final List<Simple> simples;
    private final NestedIn nestedIn;

    NestedOut(List<Simple> simples, NestedIn nestedIn) {
      this.simples = simples;
      this.nestedIn = nestedIn;
    }
    NestedOut(long id, List<Simple> simples, NestedIn nestedIn) {
      this._id = id;
      this.simples = simples;
      this.nestedIn = nestedIn;
    }

    private static BsonDocument random(Integer id) {
      int i = r.nextInt(10);
      ArrayList<Simple> simples = new ArrayList<>();
      for (int c = 0; c < i; c++) {
        simples.add(Simple.random());
      }
      if (id == null) {
        return new NestedOut(simples, NestedIn.random()).toDoc();
      }
      return new NestedOut(id, simples, NestedIn.random()).toDoc();
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
