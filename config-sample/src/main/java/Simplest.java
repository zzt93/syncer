import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;
import com.github.zzt93.syncer.data.util.SyncUtil;

import java.util.List;

/**
 * @author zzt
 */
public class Simplest implements MethodFilter {
  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    SyncUtil.unsignedByte(sync, "tinyint");
    SyncUtil.unsignedByte(sync, "type");
  }
}
