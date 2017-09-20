package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.output.Output;
import com.github.zzt93.syncer.config.syncer.OutputModule;
import com.github.zzt93.syncer.input.connect.NamedThreadFactory;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class OutputStarter implements Starter<Output, List<OutputChannel>> {

  private static OutputStarter instance;
  private final OutputJob outputJob;
  private final ExecutorService service;
  private final int worker;

  private OutputStarter(Output output, OutputModule module,
      BlockingQueue<SyncData> fromFilter) throws Exception {
    Assert.isTrue(module.getWorker() <= 8, "Too many worker thread");
    Assert.isTrue(module.getWorker() > 0, "Too few worker thread");
    service = Executors
        .newFixedThreadPool(module.getWorker(), new NamedThreadFactory("syncer-output"));

    List<OutputChannel> outputChannels = fromPipelineConfig(output);
    outputJob = new OutputJob(fromFilter, outputChannels);
    worker = module.getWorker();
  }

  public static OutputStarter getInstance(Output output, OutputModule syncerOutput,
      BlockingQueue<SyncData> fromFilter) throws Exception {
    if (instance == null) {
      instance = new OutputStarter(output, syncerOutput, fromFilter);
    }
    return instance;
  }

  public void start() throws InterruptedException {
    for (int i = 0; i < worker; i++) {
      service.submit(outputJob);
    }
  }

  @Override
  public List<OutputChannel> fromPipelineConfig(Output output) throws Exception {
    return output.toOutputChannels();
  }
}
