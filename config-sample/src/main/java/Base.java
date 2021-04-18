import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;

import java.util.List;

/**
 * @author zzt
 */
public class Base implements MethodFilter {
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
      case "simple_type":
        SyncUtil.toStr(sync, "text");
        SyncUtil.unsignedByte(sync, "tinyint");
        esSuffix = "-" + ((long) sync.getId())%2;
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }
    sync.es(sync.getRepo() + esSuffix, sync.getEntity());
  }


}
