package com.github.zzt93.syncer.input.filter;

import com.github.zzt93.syncer.common.RowEvent;
import com.github.zzt93.syncer.common.SchemaMeta;

/**
 * @author zzt
 */
public class SchemaFilter implements InputFilter {

  private final SchemaMeta schemaMeta;

  public SchemaFilter(SchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  @Override
  public FilterRes decide(RowEvent e) {
    return schemaMeta.filterRow(e) ? FilterRes.ACCEPT : FilterRes.DENY;
  }
}
