package com.github.zzt93.syncer.config.code;

import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;

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
        SyncUtil.toStr(sync, "thumb_content");
        SyncUtil.toStr(sync, "content");
        break;
      case "types":
      case SIMPLE_TYPE:
        SyncUtil.toStr(sync, "text");
        SyncUtil.unsignedByte(sync, "tinyint");
        sync.addExtra("suffix", "-" + ((long) sync.getId())%2);
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }
    if (!sync.getEntity().equals(SIMPLE_TYPE)) {
      sync.addExtra("target", "test_0");
    } else {
      sync.addExtra("target", sync.getRepo());
    }
  }


}
