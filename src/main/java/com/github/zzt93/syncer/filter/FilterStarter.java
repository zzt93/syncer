package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.FilterConfig;
import com.github.zzt93.syncer.config.syncer.FilterModule;
import com.github.zzt93.syncer.input.connect.NamedThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class FilterStarter {

  private static FilterStarter instance;
  private final ExecutorService service;
  private final List<FilterJob> filterJobs;

  private FilterStarter(List<FilterConfig> filters,
      FilterModule filter, BlockingQueue<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    Assert.isTrue(filter.getWorker() <= 8, "Too many worker thread");
    Assert.isTrue(filter.getWorker() > 0, "Too few worker thread");
    service = Executors
        .newFixedThreadPool(filter.getWorker(), new NamedThreadFactory("syncer-filter"));
    filterJobs = fromPipelineConfig(filters);
  }

  private List<FilterJob> fromPipelineConfig(List<FilterConfig> filters) {
    List<FilterJob> res = new ArrayList<>();

    return res;
  }

  public static FilterStarter getInstance(List<FilterConfig> filters,
      FilterModule filter, BlockingQueue<SyncData> fromInput,
      BlockingQueue<SyncData> toOutput) {
    if (instance == null) {
      instance = new FilterStarter(filters, filter, fromInput, toOutput);
    }
    return instance;
  }

  public void start() throws InterruptedException {
    service.invokeAll(filterJobs);
  }
}
