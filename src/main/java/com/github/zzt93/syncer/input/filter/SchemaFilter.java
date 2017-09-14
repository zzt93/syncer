package com.github.zzt93.syncer.input.filter;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.zzt93.syncer.common.SchemaMeta;
import com.github.zzt93.syncer.common.TableMeta;
import com.github.zzt93.syncer.common.event.RowEvent;

/**
 * @author zzt
 */
public class SchemaFilter implements InputFilter {

  private final SchemaMeta schemaMeta;

  public SchemaFilter(SchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  @Override
  public FilterRes decide(RowEvent rowEvent) {
    TableMapEventData data = rowEvent.getTableMap();
    TableMeta table = schemaMeta.findTable(data.getDatabase(), data.getTable());
    boolean filtered = table != null && rowEvent.filterData(table.getIndex());
    return filtered ? FilterRes.ACCEPT : FilterRes.DENY;
  }
}
