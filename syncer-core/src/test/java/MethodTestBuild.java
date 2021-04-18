import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.util.Date;
import java.util.List;

/**
 * @author zzt
 */
public class MethodTestBuild implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    Date d = new Date(0);
  }

}
