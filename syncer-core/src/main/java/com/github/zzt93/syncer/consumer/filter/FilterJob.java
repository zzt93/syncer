package com.github.zzt93.syncer.consumer.filter;

import com.github.zzt93.syncer.ShutDownCenter;
import com.github.zzt93.syncer.common.LogbackLoggingField;
import com.github.zzt93.syncer.common.data.DataId;
import com.github.zzt93.syncer.common.data.EvaluationFactory;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.exception.FailureException;
import com.github.zzt93.syncer.common.thread.EventLoop;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.FailureLogConfig;
import com.github.zzt93.syncer.config.syncer.SyncerFilter;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.channel.OutputChannel;
import com.github.zzt93.syncer.consumer.output.failure.FailureEntry;
import com.github.zzt93.syncer.consumer.output.failure.FailureLog;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <ul>
 *   <li>Use single thread to do filter & mapping (i.e. CPU related work) which is fast enough and thread safe
 *  and order of event is ensured.</li>
 *  <li>Use multiple thread to do IO in output</li>
 *  <li>Use ArrayBlockingQueue to limit memory usage</li>
 * </ul>
 *
 * @see OutputChannel#output(SyncData)
 * @author zzt
 */
public class FilterJob implements EventLoop {


  private final Logger logger = LoggerFactory.getLogger(FilterJob.class);
  private final ArrayBlockingQueue<SyncData> fromInput;
  private final CopyOnWriteArrayList<OutputChannel> outputChannels;
  private final List<SyncFilter> filters;
  private final String consumerId;
  private final Ack ack;
  private final FailureLog<Object> failureLog;

  public FilterJob(String consumerId, ArrayBlockingQueue<SyncData> fromInput,
                   CopyOnWriteArrayList<OutputChannel> outputChannels,
                   List<SyncFilter> filters, Ack ack, SyncerFilter module) {
    this.consumerId = consumerId;
    this.fromInput = fromInput;
    this.outputChannels = outputChannels;
    this.filters = filters;
    this.ack = ack;
    FailureLogConfig failureLog = module.getFailureLog();
    Path path = Paths.get(module.getFilterMeta().getFailureLogDir(), consumerId);
    this.failureLog = FailureLog.getLogger(path, failureLog, new TypeToken<FailureEntry<SyncData>>() {
    });
  }

  @Override
  public void loop() {
    LinkedList<SyncData> list = new LinkedList<>();
    while (!Thread.currentThread().isInterrupted()) {
      try {
        SyncData poll = fromInput.take();
        filter(list, poll);
        addOrDiscardAck(poll, list);
        output(list);
      } catch (InterruptedException e) {
        logger.warn("[Shutting down] Filter job interrupted");
        return;
      }
    }
  }

  private void addOrDiscardAck(SyncData poll, LinkedList<SyncData> list) {
    DataId dataId = poll.getDataId();
    boolean hasOld = false;
    for (SyncData syncData : list) {
      if (dataId.equals(syncData.getDataId())) {
        hasOld = true;
      } else {
        ack.append(syncData.getSourceIdentifier(), syncData.getDataId());
      }
    }
    if (!hasOld) {
      ack.remove(poll.getSourceIdentifier(), dataId);
    }
  }

  private void filter(LinkedList<SyncData> list, SyncData poll) {
    MDC.put(LogbackLoggingField.EID, poll.getEventId());
    logger.debug("Filter SyncData: {}", poll);

    list.clear();
    list.add(poll);
    try {
      for (SyncFilter filter : filters) {
        filter.filter(list);
      }
    } catch (InvalidConfigException e) {
      logger.error("Invalid config for {}", poll, e);
      shutdown(e, outputChannels);
    } catch (Throwable e) {
      failureLog.log(poll, "Invalid config");
      logger.error("Check [input & filter] config, otherwise syncer will be blocked: {}", poll, e);
      list.clear();
    }
  }

  private void output(LinkedList<SyncData> list) {
    for (SyncData syncData : list) {
      logger.debug("Output SyncData {}", syncData);
      // TODO 2020/6/11 remove Spring EL
      syncData.setContext(EvaluationFactory.context());
      for (OutputChannel outputChannel : this.outputChannels) {
        try {
          outputChannel.output(syncData);
        } catch (InvalidConfigException e) {
          logger.error("Invalid config for {}", syncData, e);
          shutdown(e, outputChannels);
        } catch (FailureException e) {
          String err = FailureException.getErr(outputChannel, consumerId);
          failureLog.log(syncData, err);
          logger.error(err, e);
//          SyncerHealth.consumer(this.consumerId, outputChannel.id(), Health.red(err));
        } catch (Throwable e) {
          failureLog.log(syncData, "Output failed");
          logger.error("Output {} failed", syncData, e);
//          Throwables.throwIfUnchecked(e);
        }
      }
    }
  }


  private void shutdown(Exception e, List<OutputChannel> all) {
    for (OutputChannel outputChannel : all) {
      outputChannel.close();
    }
    ShutDownCenter.initShutDown(e);
  }

}
