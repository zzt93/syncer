package com.github.zzt93.syncer.data;

/**
 * @author zzt
 */
public enum SimpleEventType {
  WRITE() {
    @Override
    public String abbr() {
      return "w";
    }
  }, UPSERT() {
    @Override
    public String abbr() {
      return "w";
    }
  }, UPDATE {
    @Override
    public String abbr() {
      return "u";
    }
  }, DELETE {
    @Override
    public String abbr() {
      return "d";
    }
  };

  public abstract String abbr();
}
