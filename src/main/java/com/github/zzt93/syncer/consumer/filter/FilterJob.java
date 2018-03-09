package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.FailureException;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class FilterJob implements Runnable {

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingDeque<SyncData> fromInput;
  private final CopyOnWriteArrayList<OutputChannel> outputChannels;
  private final List<ExprFilter> filters;
  private final Ack ack;

  public FilterJob(Ack ack, BlockingDeque<SyncData> fromInput, CopyOnWriteArrayList<OutputChannel> outputChannels,
      List<ExprFilter> filters) {
    this.fromInput = fromInput;
    this.outputChannels = outputChannels;
    this.filters = filters;
    this.ack = ack;
  }

  @Override
  public void run() {
    LinkedList<SyncData> list = new LinkedList<>();
    List<OutputChannel> remove = new LinkedList<>();
    while (!Thread.interrupted()) {
      SyncData poll = null;
      try {
        list.clear();
        poll = fromInput.take();
        MDC.put(IdGenerator.EID, poll.getEventId());
        logger.debug("remove: data id: {}", poll.getDataId());

        list.add(poll);
        for (ExprFilter filter : filters) {
          filter.decide(list);
        }
      } catch (Exception e) {
        logger.error("Filter job failed with {}", poll, e);
        continue;
      }
      for (SyncData syncData : list) {
        logger.debug("foreach output: data id: {}", poll.getDataId());
        ack.append(syncData.getSourceIdentifier(), syncData.getDataId(), outputChannels.size());
        for (OutputChannel outputChannel : this.outputChannels) {
          try {
            outputChannel.output(syncData);
          } catch (FailureException e) {
            fromInput.addFirst(syncData);
            logger.error("Failure log with too many failed items, aborting this output channel", e);
            remove.add(outputChannel);
          } catch (Exception e) {
            logger.error("Output job failed", e);
          }
        }
      }
      if (!remove.isEmpty()) {
        outputChannels.removeAll(remove);
        // TODO 18/2/12 channel cleanup
        remove.clear();
      }
    }
  }
}
