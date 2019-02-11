
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;
import java.util.function.Supplier;

public class DataGenerator {

  public static void main(String[] args) {

  }

  public static void csv(BufferedOutputStream out, Supplier<List<Object>> data, int lines) throws IOException {
    for (int i = 0; i < lines; i++) {
      List<Object> line = data.get();
      StringJoiner joiner = new StringJoiner(",");
      for (Object o : line) {
        joiner.add(o.toString());
      }
      out.write(joiner.toString().getBytes());
    }
    out.flush();
    out.close();
  }

  private static Timestamp random() {
    long abs = Math.abs(r.nextLong());
    return new Timestamp(abs);
  }

  private static String random(int min, int max) {
    int l = r.nextInt(max - min) + min;
    StringBuilder sb = new StringBuilder(l);
    for (int i = 0; i < l; i++) {
      sb.append(random());
    }
    return sb.toString();
  }

  private static Random r = new Random();
  private static final int MIN = 32;
  private static final int MAX = 127;

  private static char randomAscii() {
    return (char) (MIN + r.nextInt(MAX - MIN));
  }
}