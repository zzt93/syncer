import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.util.List;

/**
 * @author zzt
 */
public class OnlyUpdatedFalse implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    SyncData sync = list.get(0);
    if (sync.isUpdate() && !sync.updated()) {
      MethodFilter.logger.info("Nothing updated {}", sync);
      list.clear();
      return;
    }
  }

}
