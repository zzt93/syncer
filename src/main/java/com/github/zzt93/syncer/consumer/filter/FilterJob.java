package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.EvaluationFactory;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.FailureException;
import com.github.zzt93.syncer.common.exception.ShutDownException;
import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.google.common.base.Throwables;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class FilterJob implements Runnable {

  private static final ThreadLocal<StandardEvaluationContext> contexts = ThreadLocal.withInitial(
      EvaluationFactory::context);

  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final BlockingDeque<SyncData> fromInput;
  private final CopyOnWriteArrayList<OutputChannel> outputChannels;
  private final List<ExprFilter> filters;
  private final Ack ack;

  public FilterJob(Ack ack, BlockingDeque<SyncData> fromInput,
      CopyOnWriteArrayList<OutputChannel> outputChannels,
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
      if (filter(list)) {
        continue;
      }
      output(list, remove);
    }
  }

  private void output(LinkedList<SyncData> list, List<OutputChannel> remove) {
    for (SyncData syncData : list) {
      logger.debug("foreach output: data id: {}", syncData.getDataId());
      ack.append(syncData.getSourceIdentifier(), syncData.getDataId(), outputChannels.size());
      for (OutputChannel outputChannel : this.outputChannels) {
        try {
          outputChannel.output(syncData);
        } catch (InterruptedException e) {
          logger.warn("Interrupt current thread {}", Thread.currentThread().getName(), e);
          shutdown(e);
        } catch (InvalidConfigException e) {
          logger.error("Invalid config for {}", syncData, e);
          shutdown(e);
        } catch (FailureException e) {
          fromInput.addFirst(syncData);
          logger.error("Failure log with too many failed items, aborting this output channel", e);
          remove.add(outputChannel);
        } catch (Throwable e) {
          logger.error("Output {} failed", syncData, e);
          Throwables.throwIfUnchecked(e);
        }
      }
      syncData.removeContext();
      contexts.remove();
    }
    failureChannelCleanUp(remove);
  }

  private void failureChannelCleanUp(List<OutputChannel> remove) {
    if (!remove.isEmpty()) {
      outputChannels.removeAll(remove);
      for (OutputChannel outputChannel : remove) {
        // TODO 18/2/12 channel cleanup
      }
      remove.clear();
    }
  }

  private boolean filter(LinkedList<SyncData> list) {
    SyncData poll = null;
    try {
      list.clear();
      poll = fromInput.takeFirst();
      // one thread share one context to save much memory
      poll.setContext(contexts.get());
      // add dataId to avoid the loss of data when exception happens when do filter
      ack.append(poll.getSourceIdentifier(), poll.getDataId(), 1);
      MDC.put(IdGenerator.EID, poll.getEventId());
      logger.debug("remove: data id: {}", poll.getDataId());

      list.add(poll);
      for (ExprFilter filter : filters) {
        filter.decide(list);
      }
    } catch (Throwable e) {
      logger.error(
          "Filter job failed with {}: check [input & filter] config, otherwise syncer will be blocked",
          poll, e);
      Throwables.throwIfUnchecked(e);
      return true;
    }
    ack.remove(poll.getSourceIdentifier(), poll.getDataId());
    return false;
  }

  private void shutdown(Exception e) {
    // TODO 18/8/3 channel cleanup
    throw new ShutDownException(e);
  }
}
