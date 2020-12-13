package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.common.util.NamedThreadFactory;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.consumer.output.FailureLogConfig;
import com.github.zzt93.syncer.config.consumer.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.consumer.output.mysql.Mysql;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.github.zzt93.syncer.consumer.output.channel.SyncWrapper;
import com.github.zzt93.syncer.consumer.output.failure.FailureEntry;
import com.github.zzt93.syncer.consumer.output.failure.FailureLog;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zzt
 */
public class MysqlChannel implements BufferedChannel<String> {

  private final Logger logger = LoggerFactory.getLogger(MysqlChannel.class);
  private final BatchBuffer<SyncWrapper<String>> batchBuffer;
  private final PipelineBatchConfig batch;
  private final JdbcTemplate jdbcTemplate;
  private final SQLMapper sqlMapper;
  private final Ack ack;
  private final FailureLog<SyncWrapper<String>> sqlFailureLog;
  private final String output;
  private final String consumerId;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ExecutorService mysqlService;

  public MysqlChannel(Mysql mysql, SyncerOutputMeta outputMeta, Ack ack) {
    MysqlConnection connection = mysql.getConnection();
    jdbcTemplate = new JdbcTemplate(connection.dataSource());
    batchBuffer = new BatchBuffer<>(mysql.getBatch());
    sqlMapper = new SQLMapper(mysql.getRowMapping());
    this.batch = mysql.getBatch();
    this.ack = ack;
    FailureLogConfig failureLog = mysql.getFailureLog();
    sqlFailureLog = FailureLog.getLogger(Paths.get(outputMeta.getFailureLogDir(), connection.connectionIdentifier()),
        failureLog, new TypeToken<FailureEntry<SyncWrapper<String>>>() {
        });
    output = connection.connectionIdentifier();
    consumerId = mysql.getConsumerId();
    mysqlService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(10),
        new NamedThreadFactory("syncer-" + connection.connectionIdentifier() + "-output-mysql"),
        new ThreadPoolExecutor.DiscardPolicy());
  }

  @Override
  public boolean output(SyncData event) throws InterruptedException {
    if (closed.get()) {
      return false;
    }

    String sql = sqlMapper.map(event);
    boolean add = batchBuffer.add(new SyncWrapper<>(event, sql));
    logger.debug("Add {}, {}", sql, add);

    mysqlService.submit(() -> {
      try {
        BufferedChannel.super.flushAndSetFlushDone(true);
      } catch (InterruptedException e) {
        logger.warn("[Shutting down] Filter job interrupted");
      }
    });

    return add;
  }

  @Override
  public String des() {
    return "MysqlChannel{" +
        "jdbcTemplate=" + jdbcTemplate +
        '}';
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    BufferedChannel.super.close();
  }

  @Override
  public String id() {
    return output;
  }

  @Override
  public long getDelay() {
    return batch.getDelay();
  }

  @Override
  public TimeUnit getDelayUnit() {
    return batch.getDelayTimeUnit();
  }

  @Override
  public boolean flush() throws InterruptedException {
    List<SyncWrapper<String>> sqls = batchBuffer.flush();
    batchAndRetry(sqls);
    return sqls != null;
  }

  private void batchAndRetry(List<SyncWrapper<String>> sqls) throws InterruptedException {
    if (sqls == null) {
      return;
    }
    String[] sqlStatement = sqls.stream().map(SyncWrapper::getData).toArray(String[]::new);
    logger.info("Flush batch({})", sqls.size());
    if (logger.isDebugEnabled()) {
      logger.debug("Sending {}", Arrays.toString(sqlStatement));
    }
    long sleepInSecond = 1;
    while (!Thread.currentThread().isInterrupted()) {
      try {
        jdbcTemplate.batchUpdate(sqlStatement);
        ackSuccess(sqls);
        return;
      } catch (CannotGetJdbcConnectionException e) {
        String error = "Fail to connect to DB, will retry in {} second(s)";
        logger.error(error, sleepInSecond, e);
        SyncerHealth.consumer(consumerId, output, Health.red(error));
        sleepInSecond = FallBackPolicy.POW_2.sleep(sleepInSecond);
      } catch (DataAccessException e) {
        retryFailed(sqls, e);
        return;
      }
    }
  }

  @Override
  public boolean flushIfReachSizeLimit() throws InterruptedException {
    List<SyncWrapper<String>> sqls = batchBuffer.flushIfReachSizeLimit();
    batchAndRetry(sqls);
    return sqls != null;
  }

  @Override
  public void setFlushDone() {
    batchBuffer.flushDone();
  }

  @Override
  public void ackSuccess(List<SyncWrapper<String>> aim) {
    for (SyncWrapper wrapper : aim) {
      ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  @Override
  public void retryFailed(List<SyncWrapper<String>> sqls, Throwable e) {
    Throwable cause = e.getCause();
    /*
      After a command in a batch update fails to execute properly and a BatchUpdateException is thrown,
      the driver may or may not continue to process the remaining commands in the batch.
      If the driver continues processing after a failure,
      the array returned by the method BatchUpdateException.getUpdateCounts will have an element for every command in the batch
      rather than only elements for the commands that executed successfully before the error.
      In the case where the driver continues processing commands,
      the array element for any command that failed is Statement.EXECUTE_FAILED.
     */
    /*
      Mysql with `rewriteBatchedStatements=true` not continue processing and return [-1, Statement.EXECUTE_FAILED, ...] in case of BatchUpdateException
     */
    if (!(cause instanceof BatchUpdateException)) {
      logger.error("Unknown exception", e);
      throw new IllegalStateException();
    }

    LinkedList<SyncWrapper<String>> tmp = new LinkedList<>();
    int[] updateCounts = ((BatchUpdateException) cause).getUpdateCounts();
    for (int i = 0; i < updateCounts.length; i++) {
      SyncWrapper<String> stringSyncWrapper = sqls.get(i);
      if (succ(updateCounts[i])) {
        ack.remove(stringSyncWrapper.getSourceId(), stringSyncWrapper.getSyncDataId());
        continue;
      }
      // TODO 2019-10-28 maybe diff error reason!!!
      ErrorLevel level = level(e, stringSyncWrapper, batch.getMaxRetry());
      if (level.retriable()) {
        tmp.add(stringSyncWrapper);
        continue;
      } else {
        switch (level) {
          case MAX_TRY_EXCEED:
          case SYNCER_BUG: // count as failure then write a log, so no break
            sqlFailureLog.log(stringSyncWrapper, cause.getMessage());
            logger.error("Met {} in {}", level, stringSyncWrapper, cause);
            break;
          case WARN: // not count WARN as failure item
            logger.error("Met [{}] in {}", cause.getMessage(), stringSyncWrapper);
            break;
        }
      }
      ack.remove(stringSyncWrapper.getSourceId(), stringSyncWrapper.getSyncDataId());
    }
    batchBuffer.addAllInHead(tmp);
  }

  private boolean succ(long updateCount) {
    // TODO 2019-10-28 -1?
    return updateCount != Statement.EXECUTE_FAILED;
  }

  @Override
  public ErrorLevel level(Throwable e, SyncWrapper wrapper, int maxTry) {
    /*
     * Possible reasons for DuplicateKey
     * 1. the first failed, the second succeed. Then restart, then the second will send again and cause this
     * 2. duplicate entry in binlog file: load data into db multiple time
     * 3. the data is sync to mysql but not receive response before syncer shutdown
     */
    if (e instanceof DuplicateKeyException) {
      return ErrorLevel.WARN;
    }
    if (e instanceof BadSqlGrammarException) {
      return ErrorLevel.SYNCER_BUG;
    }
    return BufferedChannel.super.level(e, wrapper, maxTry);
  }

  @Override
  public boolean checkpoint() {
    return ack.flush();
  }
}
