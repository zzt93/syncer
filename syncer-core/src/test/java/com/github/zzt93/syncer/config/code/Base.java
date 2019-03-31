package com.github.zzt93.syncer.config.code;

import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * @author zzt
 */
public class Base implements MethodFilter {
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
      case "simple_type":
        SyncUtil.toStr(sync, "text");
        SyncUtil.unsignedByte(sync, "tinyint");
        sync.addExtra("suffix", "-" + ((long) sync.getId())%2);
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }
  }

  @Test
  public void unsignedInt() {
    HashMap<String, Object> sync = new HashMap<>();
    int i = 131;
    sync.put("tinyint", (int)(byte) i);
    Assert.assertEquals(Byte.toUnsignedInt((byte)(int) sync.get("tinyint")), i);
    Assert.assertNotEquals(sync.get("tinyint"), i);
  }


}
