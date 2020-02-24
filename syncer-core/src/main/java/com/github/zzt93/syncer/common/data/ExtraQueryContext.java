package com.github.zzt93.syncer.common.data;

import java.util.LinkedList;

public class ExtraQueryContext {

  private LinkedList<ExtraQuery> queries;

  public ExtraQuery add(ExtraQuery e) {
    if (queries == null) {
      queries = new LinkedList<>();
    }
    queries.add(e);
    return e;
  }

  public LinkedList<ExtraQuery> getQueries() {
    return queries;
  }

}
