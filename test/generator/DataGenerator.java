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
  private static final String SQL = "sql";
  private static final Supplier<Object> idSupplier = () -> "id";
  private static final boolean incId = true;
  private static int index = CREATE_TABLE.length();
  private static Random r = new Random();
  private static DateFormat mysqlDefault = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws IOException {
    String outDir = args[0];
    String sqlFile = args[1];
    long lines = Long.parseLong(args[2]);
    long idStart = args.length >= 4 ? Long.parseUnsignedLong(args[3]) : 0;
    for (Map.Entry<String, List<Col>> e : tables(sqlFile).entrySet()) {
      String tableName = e.getKey();
      if (tableName.endsWith("_bak")) {
        continue;
      }
      Path path = Paths.get(outDir, CSV, sqlFile.split("\\.")[0], tableName + "." + CSV);
      Files.createDirectories(path.getParent());
      System.out.println("Generate " + path);
      PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(path.toFile())));
      csv(out, e.getValue(), lines, idStart);
      out.close();

      path = Paths.get(outDir, SQL, sqlFile.split("\\.")[0], tableName + "." + SQL);
      Files.createDirectories(path.getParent());
      System.out.println("Generate " + path);
      out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(path.toFile())));
      sql(out, e.getValue(), lines, Type.UPDATE_TO_SAME_VALUE, tableName, idStart);
      sql(out, e.getValue(), lines, Type.DELETE, tableName, idStart);
      out.close();
    }
  }

  private static Map<String, List<Col>> tables(String sqlFile) throws IOException {
    List<String> lines = Files.readAllLines(Paths.get(sqlFile));
    Map<String, List<Col>> res = new HashMap<>();
    List<Col> table = null;
    for (String line : lines) {
      if (line.toUpperCase().startsWith(CREATE_TABLE)) {
        table = new LinkedList<>();
        res.put(getTableName(line), table);
      } else {
        Col col = parseLine(line);
        if (col != null) {
          if (table == null) throw new IllegalArgumentException(sqlFile);
          table.add(col);
        }
      }
    }
    return res;
  }

  private static Col parseLine(String line) {
    String trim = line.trim();
    if (trim.length() == 0) {
      return null;
    }
    String[] tokens = trim.split("\\s+");
    if (tokens.length < 2) throw new IllegalArgumentException(line);
    String col = tokens[0].trim();
    if (incId && col.equals("`id`")) {
      return new Col(idSupplier, idSupplier, "id");
    }
    Supplier<Object> csv = null, sql = null;
    String[] type = getType(tokens[1]);
    switch (type[0]) {
      case "tinyint":
        int max = type.length > 1 ? (int) Math.pow(2, Double.parseDouble(type[1])) : Byte.MAX_VALUE;
        if (tokens.length > 2 && tokens[2].toUpperCase().equals(UNSIGNED)) {
          max *= 2;
        }
        int finalMax = max;
        csv = () -> r.nextInt(finalMax);
        break;
      case "bigint":
        if (tokens.length > 2 && tokens[2].toUpperCase().equals(UNSIGNED)) {
          csv = () -> Math.abs(r.nextLong());
          break;
        }
        csv = () -> r.nextLong();
        break;
      case "char":
      case "varchar":
        csv = () -> random(1, Integer.parseInt(type[1]), false);
        sql = () -> random(1, Integer.parseInt(type[1]), true);
        break;
      case "text":
      case "longtext":
        csv = () -> random(1, 300, false);
        sql = () -> random(1, 300, true);
        break;
      case "decimal":
        final int[] val = new int[]{r.nextInt()};
        if (tokens.length > 2 && tokens[2].toUpperCase().equals(UNSIGNED)) {
          val[0] = Math.abs(val[0]);
        }
        csv = () -> new BigDecimal(val[0]).movePointLeft(2);
        break;
      case "double":
        csv = () -> r.nextFloat();
        break;
      case "timestamp":
        if (type.length > 1) {
          csv = DataGenerator::randomTimestamp;
          sql = () -> "'" + randomTimestamp() + "'";
          break;
        }
        csv = () -> mysqlDefault.format(randomTimestamp());
        sql = () -> "'" + mysqlDefault.format(randomTimestamp()) + "'";
        break;
    }
    if (csv != null) {
      return new Col(csv, sql == null ? csv : sql, removeQuote(col));
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
    return removeQuote(sub);
  }

  private static String removeQuote(String sub) {
    if (sub.charAt(0) == '`') {
      if (sub.charAt(sub.length() - 1) == '`') {
        return sub.substring(1, sub.length() - 1);
      } else {
        throw new IllegalArgumentException(sub);
      }
    }
    return sub;
  }

  private static void csv(PrintWriter out, List<Col> data, long lines, long idStart) {
    for (int i = 0; i < lines; i++) {
      List<Object> line = new LinkedList<>();
      for (Col col : data) {
        Supplier<Object> supplier = col.csv;
        if (supplier == idSupplier) {
          line.add(getId(idStart, i));
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
  }

  private static void sql(PrintWriter out, List<Col> cols, long lines, Type type, String tableName, long idStart) {
    assert cols.size() > 1;
    switch (type) {
      case UPDATE_TO_SAME_VALUE:
        StringBuilder sql = new StringBuilder("UPDATE `");
        sql.append(tableName).append("` SET ");
        StringJoiner joiner = new StringJoiner(",");
        Collections.shuffle(cols);
        int c = 0;
        for (int i = 0; i < cols.size(); i++) {
          Col col = cols.get(i);
          if (col.sql == idSupplier) {
            c = i;
            break;
          } else {
            joiner.add("`" + col.name + "`=" + col.sql.get());
          }
        }
        if (c == 0) {
          Col col = cols.get(1);
          joiner.add("`" + col.name + "`=" + col.sql.get());
        }
        sql.append(joiner).append(" where id in (");
        for (long i = 0; i < lines; i++) {
          sql.append(getId(idStart, i)).append(',');
        }
        sql.deleteCharAt(sql.length()-1).append(");");
        out.println(sql);
        break;
      case UPDATE_TO_RANDOM_VALUE:
        break;
      case UPDATE_RADDOM_COL_TO_RANDOM_VALUE:
        break;
      case DELETE:
        out.print("DELETE from `");
        out.print(tableName);
        out.println("` WHERE RAND() < 0.5;");
        break;
    }
    out.flush();
  }

  private static long getId(long idStart, long i) {
    return idStart + i + 1;
  }

  private static String random(int min, int max, boolean hasQuote) {
    int l = r.nextInt(max - min) + min;
    StringBuilder sb = new StringBuilder(l + 2);
    if (hasQuote) {
      sb.append("'");
    }
    for (int i = 0; i < l; i++) {
      char c = randomAscii();
      while (c == ',' || c == '\\' || (hasQuote && c == '\'')) {
        c = randomAscii();
      }
      sb.append(c);
    }
    if (hasQuote) {
      sb.append("'");
    }
    return sb.toString();
  }

  private static char randomAscii() {
    return (char) (MIN + r.nextInt(MAX - MIN));
  }

  private enum Type {
    UPDATE_TO_SAME_VALUE, UPDATE_TO_RANDOM_VALUE, UPDATE_RADDOM_COL_TO_RANDOM_VALUE, DELETE
  }

  private static class Col {
    private Supplier<Object> csv;
    private Supplier<Object> sql;
    private String name;

    Col(Supplier<Object> csv, Supplier<Object> sql, String name) {
      this.csv = csv;
      this.sql = sql;
      this.name = name;
    }

  }
}