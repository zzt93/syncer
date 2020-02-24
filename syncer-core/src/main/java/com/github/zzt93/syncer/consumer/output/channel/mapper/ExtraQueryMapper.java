package com.github.zzt93.syncer.consumer.output.channel.mapper;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.common.data.ExtraQueryContext;
import com.github.zzt93.syncer.common.data.Mapper;

import java.util.Map;

/**
 * @author zzt
 */
public interface ExtraQueryMapper extends Mapper<ExtraQuery, Map<String, Object>> {

  default void parseExtraQueryContext(ExtraQueryContext extraQueryContext) {
    if (extraQueryContext == null) {
      return;
    }
    assert extraQueryContext.getQueries() != null;
    for (ExtraQuery query : extraQueryContext.getQueries()) {
      map(query);
    }
  }

}
