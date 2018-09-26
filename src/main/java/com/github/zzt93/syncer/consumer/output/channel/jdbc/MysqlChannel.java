package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.common.util.FallBackPolicy;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatchConfig;
import com.github.zzt93.syncer.config.pipeline.output.mysql.Mysql;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.ack.FailureEntry;
import com.github.zzt93.syncer.consumer.ack.FailureLog;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.google.gson.reflect.TypeToken;
import com.mysql.jdbc.Driver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
  private final String id;

  public MysqlChannel(Mysql mysql, SyncerOutputMeta outputMeta, Ack ack) {
    MysqlConnection connection = mysql.getConnection();
    id = connection.connectionIdentifier();
    jdbcTemplate = new JdbcTemplate(dataSource(connection, Driver.class.getName()));
    batchBuffer = new BatchBuffer<>(mysql.getBatch());
    sqlMapper = new NestedSQLMapper(mysql.getRowMapping(), jdbcTemplate);
    this.batch = mysql.getBatch();
    this.ack = ack;
    FailureLogConfig failureLog = mysql.getFailureLog();
    try {
      sqlFailureLog = new FailureLog<>(
          Paths.get(outputMeta.getFailureLogDir(), connection.initIdentifier()),
          failureLog, new TypeToken<FailureEntry<SyncWrapper<String>>>() {
      });
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Impossible", e);
    }
  }

  private DataSource dataSource(MysqlConnection connection, String className) {
    HikariConfig config = connection.toConfig();
    config.setDriverClassName(className);
    // A value less than zero will not bypass any connection attempt and validation during startup,
    // and therefore the pool will start immediately
    config.setInitializationFailTimeout(-1);
    return new HikariDataSource(config);
  }

  @Override
  public boolean output(SyncData event) throws InterruptedException {
    String sql = sqlMapper.map(event);
    logger.info("Convert event to sql: {}", sql);
    boolean add = batchBuffer
        .add(new SyncWrapper<>(event, sql));
    flushIfReachSizeLimit();
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

  }

  @Override
  public String id() {
    return id;
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
  public void flush() throws InterruptedException {
    List<SyncWrapper<String>> sqls = batchBuffer.flush();
    if (sqls.size() != 0) {
      logger.debug("Flush batch of {}", sqls);
      batchAndRetry(sqls);
    }
  }

  private void batchAndRetry(List<SyncWrapper<String>> sqls) throws InterruptedException {
    String[] sqlStatement = sqls.stream().map(SyncWrapper::getData).toArray(String[]::new);
    logger.info("Sending to {}", Arrays.toString(sqlStatement));
    long sleepInSecond = 1;
    while (true) {
      try {
        jdbcTemplate.batchUpdate(sqlStatement);
        ackSuccess(sqls);
        return;
      } catch (CannotGetJdbcConnectionException e) {
        logger.error("Fail to connect to DB, will retry in {} second(s)", sleepInSecond, e);
        sleepInSecond = FallBackPolicy.POW_2.next(sleepInSecond, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(sleepInSecond);
      } catch (DataAccessException e) {
        retryFailed(sqls, e);
        return;
      }
    }
  }

  @Override
  public void flushIfReachSizeLimit() throws InterruptedException {
    List<SyncWrapper<String>> sqls = batchBuffer.flushIfReachSizeLimit();
    if (sqls != null) {
      logger.debug("Flush when reach size limit, send batch of {}", sqls);
      batchAndRetry(sqls);
    }
  }

  @Override
  public void ackSuccess(List<SyncWrapper<String>> aim) {
    for (SyncWrapper wrapper : aim) {
      ack.remove(wrapper.getSourceId(), wrapper.getSyncDataId());
    }
  }

  @Override
  public void retryFailed(List<SyncWrapper<String>> sqls, Exception e) {
    Throwable cause = e.getCause();
    if (cause instanceof BatchUpdateException) {
      int[] updateCounts = ((BatchUpdateException) cause).getUpdateCounts();
      for (int i = 0; i < updateCounts.length; i++) {
        SyncWrapper<String> stringSyncWrapper = sqls.get(i);
        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
          if (retriable(e)) {
            if (stringSyncWrapper.retryCount() < batch.getMaxRetry()) {
              batchBuffer.addFirst(stringSyncWrapper);
            } else {
              logger.error("Max retry exceed, write '{}' to failure log", stringSyncWrapper, cause);
              sqlFailureLog.log(stringSyncWrapper, cause.getMessage());
              ack.remove(stringSyncWrapper.getSourceId(), stringSyncWrapper.getSyncDataId());
            }
          } else {
            logger.error("Met non-retriable error in {}, write to failure log", stringSyncWrapper,
                cause);
            sqlFailureLog.log(stringSyncWrapper, cause.getMessage());
            ack.remove(stringSyncWrapper.getSourceId(), stringSyncWrapper.getSyncDataId());
          }
        } else {
          ack.remove(stringSyncWrapper.getSourceId(), stringSyncWrapper.getSyncDataId());
        }
      }
    }
  }

  @Override
  public boolean retriable(Exception e) {
    /*
     * Possible reasons for DuplicateKey
     * 1. the first failed, the second succeed. Then restart, then the second will send again and cause this
     * 2. duplicate entry in binlog file: load data into db multiple time
     * 3. the data is sync to mysql but not receive response before syncer shutdown
     */
    return !(e instanceof DuplicateKeyException || e instanceof BadSqlGrammarException);
  }
}
