import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.util.List;

/**
 * @author zzt
 */
public class HBaseFilter implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    if (sync.getEntity().equals("order_view")) {
      sync.hBaseTable("test").columnFamily("tags");
    }
  }


}
