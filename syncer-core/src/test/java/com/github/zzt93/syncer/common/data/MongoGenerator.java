package com.github.zzt93.syncer.common.data;

import com.github.zzt93.syncer.common.util.RandomDataUtil;
import com.google.gson.Gson;
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

/**
 * @author zzt
 */
public class MongoGenerator {


  private static Gson gson = new Gson();
  private static Random r = new Random();

  @Test
  public void generate() throws FileNotFoundException {
    int num = Integer.parseInt(System.getProperty("num"));
    String fileName = System.getProperty("fileName");
    PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(fileName)));
    List<NestedOut> res = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      res.add(NestedOut.random());
    }
    out.print(gson.toJson(res));
    out.flush();
    out.close();
  }

  private static class Simple {
    private long id;
    private byte tinyint;
    private long bigint;
    private char Char;
    private String varchar;
    private BigDecimal decimal;
    private double aDouble;
    private Timestamp timestamp;

    Simple(long id, byte tinyint, long bigint, char aChar, String varchar, BigDecimal decimal, double aDouble, Timestamp timestamp) {
      this.id = id;
      this.tinyint = tinyint;
      this.bigint = bigint;
      Char = aChar;
      this.varchar = varchar;
      this.decimal = decimal;
      this.aDouble = aDouble;
      this.timestamp = timestamp;
    }

    private static Simple random() {
      return new Simple(
        r.nextLong(), RandomDataUtil.randomByte(), r.nextLong(), RandomDataUtil.randomAscii(), randomStr(),
        RandomDataUtil.randomDecimal(), r.nextDouble() + r.nextInt(), RandomDataUtil.randomTimestamp()
      );
    }
  }

  private static String randomStr() {
    return RandomDataUtil.random(2, 10, true);
  }

  private static class NestedIn {
    private long id;
    private Date time;
    private String currency;
    private String total;
    private int quantity;
    private byte type;
    private String name;
    private String unit;

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
  }

  private static class NestedOut {

    private List<Simple> simples;
    private NestedIn nestedIn;

    NestedOut(List<Simple> simples, NestedIn nestedIn) {
      this.simples = simples;
      this.nestedIn = nestedIn;
    }

    private static NestedOut random() {
      int i = r.nextInt(10);
      ArrayList<Simple> simples = new ArrayList<>();
      for (int c = 0; c < i; c++) {
        simples.add(Simple.random());
      }
      return new NestedOut(
        simples, NestedIn.random()
      );
    }
  }
}
