import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;

import java.util.List;

/**
 * @author zzt
 */
public class ColdFilter implements MethodFilter {
  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
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
        break;
      case "correctness":
        SyncUtil.unsignedByte(sync, "type");
        break;
    }
    sync.mysql(sync.getRepo(), entity + "_bak");
  }
}
