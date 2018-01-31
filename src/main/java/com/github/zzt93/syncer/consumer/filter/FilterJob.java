package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.FailureException;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author zzt
 */
public class FilterJob implements Runnable {

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingDeque<SyncData> fromInput;
  private final List<OutputChannel> outputChannels;
  private final List<ExprFilter> filters;
  private final Ack ack;

  public FilterJob(Ack ack, BlockingDeque<SyncData> fromInput, List<OutputChannel> outputChannels,
      List<ExprFilter> filters) {
    this.fromInput = fromInput;
    this.outputChannels = outputChannels;
    this.filters = filters;
    this.ack = ack;
  }

  @Override
  public void run() {
    LinkedList<SyncData> list = new LinkedList<>();
    while (!Thread.interrupted()) {
      SyncData poll;
      try {
        list.clear();
        poll = fromInput.take();
        MDC.put(IdGenerator.EID, poll.getEventId());
        ack.append(poll.getSourceIdentifier(), poll.getDataId());

        list.add(poll);
        for (ExprFilter filter : filters) {
          filter.decide(list);
        }
      } catch (Exception e) {
        logger.error("Filter job failed", e);
        continue;
      }
      for (OutputChannel outputChannel : outputChannels) {
        for (SyncData syncData : list) {
          try {
            outputChannel.output(syncData);
          } catch (FailureException e) {
            fromInput.addFirst(syncData);
            logger.error("Failure log with too many failed items, aborting", e);
            throw e;
          } catch (Exception e) {
            logger.error("Output job failed", e);
          }
        }
      }
    }
  }
}
