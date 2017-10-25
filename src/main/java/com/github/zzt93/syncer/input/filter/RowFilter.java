package com.github.zzt93.syncer.input.filter;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.SchemaMeta;
import com.github.zzt93.syncer.common.TableMeta;
import com.github.zzt93.syncer.common.ThreadSafe;
import com.github.zzt93.syncer.common.event.RowsEvent;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class RowFilter implements InputFilter {

  private final SchemaMeta schemaMeta;

  public RowFilter(SchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  @ThreadSafe(safe = SchemaMeta.class)
  @Override
  public FilterRes decide(RowsEvent rowsEvent) {
    TableMapEventData data = rowsEvent.getTableMap();
    TableMeta table = schemaMeta.findTable(data.getDatabase(), data.getTable());
    Assert.notNull(table, "[Assertion Failure] fail to find the name: " + data.getDatabase() + data.getTable());
    boolean filtered = rowsEvent.filterData(table.getIndex());
    return filtered ? FilterRes.ACCEPT : FilterRes.DENY;
  }
}
