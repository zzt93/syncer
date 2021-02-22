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
    String esSuffix = "";
    String entity = sync.getEntity();
    switch (entity) {
      case "news":
        SyncUtil.unsignedByte(sync, "plate_sub_type");
      case "toCopy":
        SyncUtil.toStr(sync, "thumb_content");
        SyncUtil.toStr(sync, "content");
        break;
      case "types":
      case "simple_type":
        SyncUtil.toStr(sync, "text");
        SyncUtil.unsignedByte(sync, "tinyint");
        esSuffix =  "-" + ((long) sync.getId())%2;
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }
    sync.es(sync.getRepo() + esSuffix, sync.getEntity()).mysql(sync.getRepo(), entity + "_bak");

    if (entity.equals("toDiscard")) { /* clear test */
      list.clear();
    } else if (entity.equals("toCopy")) { /* copy test */
      SyncData copy = sync.copyMeta().es(sync.getRepo() + esSuffix, sync.getEntity()).mysql(sync.getRepo(), entity + "_bak")
          .setId(((Number) sync.getId()).longValue() + Integer.MAX_VALUE);
      copy.getFields().putAll(sync.getFields());
      list.add(copy);
    }
  }


}
