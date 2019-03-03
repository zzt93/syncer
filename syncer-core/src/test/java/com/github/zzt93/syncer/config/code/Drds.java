package com.github.zzt93.syncer.config.code;

import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.util.List;

/**
 * @author zzt
 */
public class Drds implements MethodFilter {

  private static final String SIMPLE_TYPE = "simple_type";

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    sync.addExtra("suffix", "");
    switch (sync.getEntity()) {
      case "news":
        sync.updateField("thumb_content", new String((byte[]) sync.getField("thumb_content"))).updateField("content", new String((byte[]) sync.getField("content")));
        break;
      case "types":
      case SIMPLE_TYPE:
        sync.updateField("text", new String((byte[]) sync.getField("text"))).updateField("tinyint", Byte.toUnsignedInt((byte)(int) sync.getField("tinyint")));
        sync.addExtra("suffix", "-" + ((int) sync.getField("tinyint"))/128);
        break;
      case "correctness":
        sync.updateField("type", Byte.toUnsignedInt((byte)(int) sync.getField("type")));
        break;
    }
    if (!sync.getEntity().equals(SIMPLE_TYPE)) {
      sync.addExtra("target", "test_0");
    } else {
      sync.addExtra("target", sync.getRepo());
    }
  }


}
