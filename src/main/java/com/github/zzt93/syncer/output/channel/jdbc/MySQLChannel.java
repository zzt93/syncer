package com.github.zzt93.syncer.output.channel.jdbc;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.common.MysqlConnection;
import com.github.zzt93.syncer.config.pipeline.output.PipelineBatch;
import com.github.zzt93.syncer.config.pipeline.output.RowMapping;
import com.github.zzt93.syncer.output.batch.BatchBuffer;
import com.github.zzt93.syncer.output.channel.BufferedChannel;
import com.mysql.jdbc.Driver;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author zzt
 */
public class MySQLChannel implements BufferedChannel {

  private final Logger logger = LoggerFactory.getLogger(MySQLChannel.class);
  private final BatchBuffer<String> batchBuffer;
  private final PipelineBatch batch;
  private final JdbcTemplate jdbcTemplate;
  private final JdbcMapper jdbcMapper;

  public MySQLChannel(MysqlConnection connection, RowMapping rowMapping,
      PipelineBatch batch) {
    jdbcTemplate = new JdbcTemplate(dataSource(connection, Driver.class.getName()));
    batchBuffer = new BatchBuffer<>(batch, String.class);
    jdbcMapper = new JdbcMapper(rowMapping);
    this.batch = batch;
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
    String sql = jdbcMapper.map(event);
    logger.debug("Convert event to sql: {}", sql);
    boolean add = batchBuffer.add(sql);
    flushIfReachSizeLimit();
    return add;
  }

  @Override
  public boolean output(List<SyncData> batch) {
    List<String> sqls = batch.stream().map(jdbcMapper::map).collect(Collectors.toList());
    boolean add = batchBuffer.addAll(sqls);
    flushIfReachSizeLimit();
    return add;
  }

  @Override
  public String des() {
    return "MySQLChannel{" +
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
    String[] sqls = batchBuffer.flush();
    if (sqls.length != 0) {
      logger.debug("Flush batch of {}", Arrays.toString(sqls));
      try {
        jdbcTemplate.batchUpdate(sqls);
      } catch (DataAccessException e) {
        logger.error("{}", Arrays.toString(sqls), e);
      }
    }
  }

  @Override
  public void flushIfReachSizeLimit() {
    String[] sqls = batchBuffer.flushIfReachSizeLimit();
    if (sqls != null) {
      logger.debug("Flush when reach size limit, send batch of {}", Arrays.toString(sqls));
      try {
        jdbcTemplate.batchUpdate(sqls);
      } catch (DataAccessException e) {
        logger.error("{}", Arrays.toString(sqls), e);
      }
    }
  }
}
