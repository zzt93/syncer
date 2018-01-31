package com.github.zzt93.syncer.consumer.output;

import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.pipeline.output.PipelineOutput;
import com.github.zzt93.syncer.config.syncer.SyncerOutput;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.batch.BatchJob;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author zzt
 */
public class OutputStarter {


  private final List<OutputChannel> outputChannels;

  public OutputStarter(PipelineOutput pipelineOutput, SyncerOutput module,
      Ack ack) throws Exception {
    workerCheck(module.getWorker());
    workerCheck(module.getBatch().getWorker());

    ScheduledExecutorService batchService = Executors
        .newScheduledThreadPool(module.getBatch().getWorker(),
            new NamedThreadFactory("syncer-batch"));

    outputChannels = pipelineOutput.toOutputChannels(ack, module.getOutputMeta());
    for (OutputChannel outputChannel : outputChannels) {
      if (outputChannel instanceof BufferedChannel) {
        BufferedChannel bufferedChannel = (BufferedChannel) outputChannel;
        long delay = bufferedChannel.getDelay();
        batchService.scheduleWithFixedDelay(new BatchJob(bufferedChannel), delay, delay,
            bufferedChannel.getDelayUnit());
      }
    }
  }

  public List<OutputChannel> getOutputChannels() {
    return outputChannels;
  }

  private void workerCheck(int worker) {
    Preconditions.checkArgument(worker <= Runtime.getRuntime().availableProcessors() * 2,
        "Too many worker thread");
    Preconditions.checkArgument(worker > 0, "Too few worker thread");
  }

}
