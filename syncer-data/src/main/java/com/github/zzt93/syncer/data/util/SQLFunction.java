package com.github.zzt93.syncer.data.util;

/**
 * @author zzt
 */
public class SQLFunction {

  private String result;

  private SQLFunction(String result) {
    this.result = result;
  }

  public static SQLFunction geomfromtext(String param) {
    return new SQLFunction("geomfromtext(" + param + ")");
  }

  @Override
  public String toString() {
    return result;
  }
}
