import com.github.zzt93.syncer.data.SyncData;
import com.github.zzt93.syncer.data.util.MethodFilter;

import java.util.List;

/**
 * @author zzt
 */
public class MethodTestDateTest implements MethodFilter {

  @Override
  public void filter(List<SyncData> list) {
    for (SyncData d : list) {
      assert d.getEventId().equals("123/1");
    }
  }

}
