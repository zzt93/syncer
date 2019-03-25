import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Supplier;

public class DataGenerator {

  private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS ";
  private static final int MIN = 32;
  private static final int MAX = 127;
  private static final String UNSIGNED = "UNSIGNED";
  private static final long EARLIEST = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
  private static final long END = Timestamp.valueOf("2028-01-01 00:00:00").getTime();
  private static final String CSV = "csv";
  private static final Supplier<Object> idSupplier = ()-> "id";
  private static int index = CREATE_TABLE.length();
  private static Random r = new Random();
  private static DateFormat mysqlDefault = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws IOException {
    String outDir = args[0];
    String sqlFile = args[1];
    long lines = Long.parseLong(args[2]);
    for (Map.Entry<String, List<Supplier<Object>>> e : tables(sqlFile).entrySet()) {
      String tableName = e.getKey();
      if (tableName.endsWith("_bak")) {
        continue;
      }
      Path path = Paths.get(outDir, CSV, sqlFile.split("\\.")[0], tableName + "." + CSV);
      Files.createDirectories(path.getParent());
      PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(path.toFile())));
      System.out.println("Generate " + tableName + " to " + path);
      csv(out, e.getValue(), lines);
    }
  }

  private static Map<String, List<Supplier<Object>>> tables(String sqlFile) throws IOException {
    List<String> lines = Files.readAllLines(Paths.get(sqlFile));
    Map<String, List<Supplier<Object>>> res = new HashMap<>();
    List<Supplier<Object>> table = null;
    for (String line : lines) {
      if (line.toUpperCase().startsWith(CREATE_TABLE)) {
        table = new LinkedList<>();
        res.put(getTableName(line), table);
      } else {
        Supplier<Object> typeSupplier = getTypeSupplier(line);
        if (typeSupplier != null) {
          if (table == null) throw new IllegalArgumentException(sqlFile);
          table.add(typeSupplier);
        }
      }
    }
    return res;
  }

  private static Supplier<Object> getTypeSupplier(String line) {
    String trim = line.trim();
    if (trim.length() == 0) {
      return null;
    }
    String[] tokens = trim.split("\\s");
    if (tokens.length < 2) throw new IllegalArgumentException(line);
    if (tokens[0].trim().equals("`id`")) {
      return idSupplier;
    }
    String[] type = getType(tokens[1]);
    switch (type[0]) {
      case "tinyint":
        int max = type.length > 1 ? (int) Math.pow(2, Double.parseDouble(type[1])) : Byte.MAX_VALUE;
        if (tokens.length > 2 && tokens[2].toUpperCase().equals(UNSIGNED)) {
          max *= 2;
        }
        int finalMax = max;
        return () -> r.nextInt(finalMax);
      case "bigint":
        if (tokens.length > 2 && tokens[2].toUpperCase().equals(UNSIGNED)) {
          return () -> Math.abs(r.nextLong());
        }
        return () -> r.nextLong();
      case "char":
      case "varchar":
        return () -> random(1, Integer.parseInt(type[1]));
      case "text":
      case "longtext":
        return () -> random(1, 300);
      case "decimal":
        return () -> new BigDecimal(r.nextInt()).movePointLeft(2);
      case "double":
        return () -> r.nextFloat();
      case "timestamp":
        if (type.length > 1) {
          return DataGenerator::randomTimestamp;
        }
        return () -> mysqlDefault.format(randomTimestamp());
    }
    return null;
  }

  private static Timestamp randomTimestamp() {
    long diff = END - EARLIEST + 1;
    long l = (long) (Math.random() * diff);
    return new Timestamp(EARLIEST + l);
  }

  private static String[] getType(String token) {
    String[] split = token.toLowerCase().replaceAll("[`(,)]", " ").split("\\s");
    LinkedList<String> res = new LinkedList<>();
    for (String s : split) {
      if (s.trim().length() > 0) {
        res.add(s);
      }
    }
    return res.toArray(new String[0]);
  }

  private static String getTableName(String line) {
    String sub = line.substring(index, line.indexOf('(')).trim();
    if (sub.charAt(0) == '`') {
      if (sub.charAt(sub.length() - 1) == '`') {
        return sub.substring(1, sub.length() - 1);
      } else {
        throw new IllegalArgumentException(line);
      }
    }
    return sub;
  }

  private static void csv(PrintWriter out, List<Supplier<Object>> data, long lines) {
    for (int i = 0; i < lines; i++) {
      List<Object> line = new LinkedList<>();
      for (Supplier<Object> supplier : data) {
        if (supplier == idSupplier) {
          line.add(i);
        } else {
          line.add(supplier.get());
        }
      }
      StringJoiner joiner = new StringJoiner(",");
      for (Object o : line) {
        joiner.add(o.toString());
      }
      out.println(joiner.toString());
    }
    out.flush();
    out.close();
  }

  private static String random(int min, int max) {
    int l = r.nextInt(max - min) + min;
    StringBuilder sb = new StringBuilder(l + 2);
    for (int i = 0; i < l; i++) {
      char c = randomAscii();
      while (c == ',' || c == '\\') {
        c = randomAscii();
      }
      sb.append(c);
    }
    return sb.toString();
  }

  private static char randomAscii() {
    return (char) (MIN + r.nextInt(MAX - MIN));
  }
}