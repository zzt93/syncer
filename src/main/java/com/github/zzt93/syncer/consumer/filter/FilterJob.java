package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.common.IdGenerator;
import com.github.zzt93.syncer.common.data.EvaluationFactory;
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
      } catch (Exception e) {
        logger.error("Filter job failed with {}: check [input & filter] config, otherwise syncer will be blocked", poll, e);
        continue;
      }
      ack.remove(poll.getSourceIdentifier(), poll.getDataId());
      for (SyncData syncData : list) {
        logger.debug("foreach output: data id: {}", syncData.getDataId());
        ack.append(syncData.getSourceIdentifier(), syncData.getDataId(), outputChannels.size());
        for (OutputChannel outputChannel : this.outputChannels) {
          try {
            outputChannel.output(syncData);
          } catch (FailureException e) {
            fromInput.addFirst(syncData);
            logger.error("Failure log with too many failed items, aborting this output channel", e);
            remove.add(outputChannel);
          } catch (Exception e) {
            logger.error("Output {} failed", syncData, e);
          }
        }
        syncData.removeContext();
        contexts.remove();
      }
      if (!remove.isEmpty()) {
        outputChannels.removeAll(remove);
        // TODO 18/2/12 channel cleanup
        remove.clear();
      }
    }
  }
}
