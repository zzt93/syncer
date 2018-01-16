package com.github.zzt93.syncer.consumer.output;

import com.github.zzt93.syncer.Starter;
import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.output.PipelineOutput;
import com.github.zzt93.syncer.config.syncer.SyncerOutput;
import com.github.zzt93.syncer.consumer.output.batch.BatchJob;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author zzt
 */
public class OutputStarter implements Starter<PipelineOutput, List<OutputChannel>> {

  private final OutputJob outputJob;
  private final ExecutorService service;
  private final ScheduledExecutorService batchService;
  private final int worker;

  public OutputStarter(PipelineOutput pipelineOutput, SyncerOutput module,
      BlockingDeque<SyncData> fromFilter) throws Exception {
    workerCheck(module.getWorker());
    workerCheck(module.getBatch().getWorker());

    service = Executors
        .newFixedThreadPool(module.getWorker(), new NamedThreadFactory("syncer-output"));
    batchService = Executors.newScheduledThreadPool(module.getBatch().getWorker(),
        new NamedThreadFactory("syncer-batch"));

    List<OutputChannel> outputChannels = fromPipelineConfig(pipelineOutput);
    for (OutputChannel outputChannel : outputChannels) {
      if (outputChannel instanceof BufferedChannel) {
        BufferedChannel bufferedChannel = (BufferedChannel) outputChannel;
        long delay = bufferedChannel.getDelay();
        batchService.scheduleWithFixedDelay(new BatchJob(bufferedChannel), delay, delay,
            bufferedChannel.getDelayUnit());
      }
    }

    outputJob = new OutputJob(fromFilter, outputChannels);
    worker = module.getWorker();
  }

  private void workerCheck(int worker) {
    Preconditions.checkArgument(worker <= 8, "Too many worker thread");
    Preconditions.checkArgument(worker > 0, "Too few worker thread");
  }

  public void start() throws InterruptedException {
    for (int i = 0; i < worker; i++) {
      service.submit(outputJob);
    }
  }

  @Override
  public List<OutputChannel> fromPipelineConfig(PipelineOutput pipelineOutput) throws Exception {
    return pipelineOutput.toOutputChannels();
  }
}
