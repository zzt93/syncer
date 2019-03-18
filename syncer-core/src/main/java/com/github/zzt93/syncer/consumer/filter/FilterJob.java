package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.EvaluationFactory;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.FailureException;
import com.github.zzt93.syncer.common.thread.EventLoop;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author zzt
 */
public class FilterJob implements EventLoop {

  private static final ThreadLocal<StandardEvaluationContext> contexts = ThreadLocal.withInitial(
      EvaluationFactory::context);

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingDeque<SyncData> fromInput;
  private final CopyOnWriteArrayList<OutputChannel> outputChannels;
  private final List<SyncFilter> filters;
  private final Ack ack;
  private final String consumerId;

  public FilterJob(String consumerId, Ack ack, BlockingDeque<SyncData> fromInput,
      CopyOnWriteArrayList<OutputChannel> outputChannels,
      List<SyncFilter> filters) {
    this.consumerId = consumerId;
    this.fromInput = fromInput;
    this.outputChannels = outputChannels;
    this.filters = filters;
    this.ack = ack;
  }

  @Override
  public void loop() {
    LinkedList<SyncData> list = new LinkedList<>();
    List<OutputChannel> remove = new LinkedList<>();
    while (!Thread.currentThread().isInterrupted()) {
      try {
        SyncData poll = fromInput.takeFirst();
        MDC.put(IdGenerator.EID, poll.getEventId());
        logger.debug("Filter SyncData: {}", poll.getDataId());

        if (filter(list, poll)) {
          continue;
        }
        output(list, remove);
      } catch (InterruptedException e) {
        logger.warn("[Shutting down] Filter job interrupted");
        return;
      }
    }
  }

  private boolean filter(LinkedList<SyncData> list, SyncData poll) {
    try {
      list.clear();
      list.add(poll);

      // add dataId to avoid the loss of data when exception happens when do filter
      ack.append(poll.getSourceIdentifier(), poll.getDataId(), 1);
      // one thread share one context to save much memory
      poll.setContext(contexts.get());

      for (SyncFilter filter : filters) {
        filter.filter(list);
      }
    } catch (InvalidConfigException e) {
      logger.error("Invalid config for {}", poll, e);
      shutdown(e, outputChannels);
    } catch (Throwable e) {
      logger.error(
          "Filter job failed with {}: check [input & filter] config, otherwise syncer will be blocked",
          poll, e);
      Throwables.throwIfUnchecked(e);
      return false;
    }
    ack.remove(poll.getSourceIdentifier(), poll.getDataId());
    return false;
  }

  private void output(LinkedList<SyncData> list, List<OutputChannel> remove)
      throws InterruptedException {
    for (SyncData syncData : list) {
      logger.debug("Output SyncData {}", syncData);
      ack.append(syncData.getSourceIdentifier(), syncData.getDataId(), outputChannels.size());
      for (OutputChannel outputChannel : this.outputChannels) {
        try {
          outputChannel.output(syncData);
        } catch (InvalidConfigException e) {
          logger.error("Invalid config for {}", syncData, e);
          shutdown(e, outputChannels);
        } catch (FailureException e) {
          fromInput.addFirst(syncData);
          String err = FailureException.getErr(outputChannel, consumerId);
          logger.error(err, e);
          SyncerHealth.consumer(this.consumerId, outputChannel.id(), Health.red(err));
          remove.add(outputChannel);
        } catch (Throwable e) {
          logger.error("Output {} failed", syncData, e);
          Throwables.throwIfUnchecked(e);
        }
      }
      syncData.recycleParseContext(contexts);
    }
    failureChannelCleanUp(remove);
  }

  private void failureChannelCleanUp(List<OutputChannel> remove) {
    if (!remove.isEmpty()) {
      outputChannels.removeAll(remove);
      for (OutputChannel outputChannel : remove) {
        outputChannel.close();
      }
      remove.clear();
    }
  }

  private void shutdown(Exception e, List<OutputChannel> all) {
    for (OutputChannel outputChannel : all) {
      outputChannel.close();
    }
    ShutDownCenter.initShutDown(e);
  }

}
