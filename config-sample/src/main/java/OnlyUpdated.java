import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;

import java.util.List;

/**
 * @author zzt
 */
public class OnlyUpdated implements MethodFilter {

  private static final String SIMPLE_TYPE = "simple_type";

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    String esSuffix = "";
    switch (sync.getEntity()) {
      case "news":
        SyncUtil.toStr(sync, "thumb_content");
        SyncUtil.toStr(sync, "content");
        break;
      case "types":
      case SIMPLE_TYPE:
        SyncUtil.toStr(sync, "text");
        SyncUtil.unsignedByte(sync, "tinyint");
        esSuffix = "-" + ((long) sync.getId())%2;
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }
    sync.es(sync.getRepo(), sync.getEntity() + esSuffix);
    if (!sync.getEntity().equals(SIMPLE_TYPE)) {
      sync.mysql("test_0", sync.getEntity() + "_bak");
    } else {
      sync.mysql(sync.getRepo(), sync.getEntity() + "_bak");
    }
  }


}
