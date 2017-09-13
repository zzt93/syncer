package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.MysqlRowEvent;
import com.github.zzt93.syncer.config.common.MetaData;
import com.github.zzt93.syncer.config.input.Schema;

/**
 * @author zzt
 */
public class SchemaFilter implements Filter<MysqlRowEvent, MysqlRowEvent> {

  private Schema schema;

  public SchemaFilter(MetaData metaData, Schema schema) {
    this.schema = schema;
  }

  @Override
  public MysqlRowEvent decide(MysqlRowEvent e) {
    return null;
  }
}
