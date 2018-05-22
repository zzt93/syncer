package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.common.data.SyncWrapper;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.output.FailureLogConfig;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.mysql.Mysql;
import com.github.zzt93.syncer.config.syncer.SyncerOutputMeta;
import com.github.zzt93.syncer.consumer.ack.Ack;
import com.github.zzt93.syncer.consumer.ack.FailureEntry;
import com.github.zzt93.syncer.consumer.ack.FailureLog;
import com.github.zzt93.syncer.consumer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.consumer.output.channel.BufferedChannel;
import com.google.gson.reflect.TypeToken;
import com.mysql.jdbc.Driver;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author zzt
 */
public class MysqlChannel implements BufferedChannel<String> {

  private final Logger logger = LoggerFactory.getLogger(MysqlChannel.class);
  private final BatchBuffer<SyncWrapper<String>> batchBuffer;
  private final PipelineBatch batch;
  private final JdbcTemplate jdbcTemplate;
  private final SQLMapper sqlMapper;
  private final Ack ack;
  private final FailureLog<SyncWrapper<String>> sqlFailureLog;

  public MysqlChannel(Mysql mysql, SyncerOutputMeta outputMeta, Ack ack) {
    MysqlConnection connection = mysql.getConnection();
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
    DataSourceProperties properties = new DataSourceProperties();
    properties.setUrl(connection.toConnectionUrl());
    properties.setUsername(connection.getUser());
    properties.setPassword(connection.getPassword());
//    properties.setDriverClassName(className);

    DataSource dataSource = (DataSource) properties.initializeDataSourceBuilder()
        .type(DataSource.class).build();
    DatabaseDriver databaseDriver = DatabaseDriver
        .fromJdbcUrl(properties.determineUrl());
    String validationQuery = databaseDriver.getValidationQuery();
    if (validationQuery != null) {
      dataSource.setTestOnBorrow(true);
      dataSource.setValidationQuery(validationQuery);
    }
    return dataSource;
  }

  @Override
  public boolean output(SyncData event) {
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
  public long getDelay() {
    return batch.getDelay();
  }

  @Override
  public TimeUnit getDelayUnit() {
    return batch.getDelayTimeUnit();
  }

  @Override
  public void flush() {
    List<SyncWrapper<String>> sqls = batchBuffer.flush();
    if (sqls.size() != 0) {
      logger.debug("Flush batch of {}", sqls);
      batchAndRetry(sqls);
    }
  }

  private void batchAndRetry(List<SyncWrapper<String>> sqls) {
    try {
      String[] sqlStatement = sqls.stream().map(SyncWrapper::getData).toArray(String[]::new);
      logger.info("Sending to {}", Arrays.toString(sqlStatement));
      jdbcTemplate.batchUpdate(sqlStatement);
      ackSuccess(sqls);
    } catch (DataAccessException e) {
      retryFailed(sqls, e);
    }
  }

  @Override
  public void flushIfReachSizeLimit() {
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
              sqlFailureLog.log(stringSyncWrapper, e);
              ack.remove(stringSyncWrapper.getSourceId(), stringSyncWrapper.getSyncDataId());
            }
          } else {
            logger.error("Met non-retriable error in {}, write to failure log", stringSyncWrapper,
                cause);
            sqlFailureLog.log(stringSyncWrapper, e);
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
     * Two possible reasons for DuplicateKey
     * 1. the first failed, the second succeed. Then restart, then the second will send again and cause this
     * 2. duplicate entry in binlog file: load data into db multiple time
     */
    return !(e instanceof DuplicateKeyException || e instanceof BadSqlGrammarException);
  }
}
