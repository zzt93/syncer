package com.github.zzt93.syncer.config.code;

import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;

import java.util.List;

/**
 * @author zzt
 */
public class AckTest implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    sync.addExtra("suffix", "");
    String entity = sync.getEntity();
    switch (entity) {
      case "news":
      case "toCopy":
        SyncUtil.toStr(sync, "thumb_content");
        SyncUtil.toStr(sync, "content");
        break;
      case "types":
      case "simple_type":
        SyncUtil.toStr(sync, "text");
        SyncUtil.unsignedByte(sync, "tinyint");
        sync.addExtra("suffix", "-" + ((long) sync.getId())%2);
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }

    if (entity.equals("toDiscard")) { /* clear test */
      list.clear();
    } else if (entity.equals("toCopy")) { /* copy test */
      SyncData copy = sync.copyMeta(0).setRepo(sync.getRepo()).setEntity(sync.getEntity())
          .setId(((Number) sync.getId()).longValue() + Integer.MAX_VALUE);
      copy.getFields().putAll(sync.getFields());
      copy.addExtra("suffix", "");
      list.add(copy);
    }
  }


}
