package com.github.zzt93.syncer.common.data.mem;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncDataTestUtil;
import com.github.zzt93.syncer.instrument.InstrumentationAgent;

import java.util.ArrayList;
import java.util.List;

import static jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize;

/**
 * cd instrumentation
 * mvn package
 * vmOption: -javaagent:"./instrumentation/target/instrumentation-1.0-SNAPSHOT.jar"
 * @author zzt
 */
public class SizeOfTest {


  private static void printObjectSize(Object object) {
    System.out.println("Object type: " + object.getClass() +
        ", size: " + getObjectSize(object) + " bytes");
    System.out.println("Object type: " + object.getClass() +
        ", size: " + InstrumentationAgent.getObjectSize(object) + " bytes");
  }


  public static void main(String[] arguments) {
    printObjectSize(SyncDataTestUtil.write("test_0", "user"));
    printObjectSize(SyncData.class);

    String emptyString = "";
    String string = "Estimating Object Size Using Instrumentation";
    String[] stringArray = { emptyString, string, "com.baeldung" };
    String[] anotherStringArray = new String[100];
    List<String> stringList = new ArrayList<>();
    StringBuilder stringBuilder = new StringBuilder(100);
    int maxIntPrimitive = Integer.MAX_VALUE;
    int minIntPrimitive = Integer.MIN_VALUE;
    Integer maxInteger = Integer.MAX_VALUE;
    Integer minInteger = Integer.MIN_VALUE;
    long zeroLong = 0L;
    double zeroDouble = 0.0;
    boolean falseBoolean = false;
    Object object = new Object();

    class EmptyClass {
    }
    EmptyClass emptyClass = new EmptyClass();

    class StringClass {
      public String s;
    }
    StringClass stringClass = new StringClass();

    printObjectSize(emptyString);
    printObjectSize(string);
    printObjectSize(stringArray);
    printObjectSize(anotherStringArray);
    printObjectSize(stringList);
    printObjectSize(stringBuilder);
    printObjectSize(maxIntPrimitive);
    printObjectSize(minIntPrimitive);
    printObjectSize(maxInteger);
    printObjectSize(minInteger);
    printObjectSize(zeroLong);
    printObjectSize(zeroDouble);
    printObjectSize(falseBoolean);
    printObjectSize(Day.TUESDAY);
    printObjectSize(object);
    printObjectSize(emptyClass);
    printObjectSize(stringClass);
  }

  public enum Day {
    MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY
  }
}
