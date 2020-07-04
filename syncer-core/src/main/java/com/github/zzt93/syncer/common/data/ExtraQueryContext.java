package com.github.zzt93.syncer.common.data;

import lombok.ToString;

import java.util.LinkedList;

@ToString
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
