import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.data.MethodFilter;
import com.github.zzt93.syncer.data.SyncFilter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MethodFilterTemplate implements SyncFilter<SyncData> {

  private final Logger logger = LoggerFactory.getLogger(MethodFilter.class);

  @Override
  public void filter(List<SyncData> list) {/*TODO*/}

}
