package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.consumer.output.channel.ExtraQueryMapper;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zzt
 */
public class JdbcNestedQueryMapper implements ExtraQueryMapper {

  private static final String SELECT_3_FROM_0_1_WHERE_2 = "select ?3 from `?0`.`?1` where ?2";

  @Override
  public Map<String, Object> map(ExtraQuery extraQuery) {
    String[] select = extraQuery.getSelect();
    String sql = ParameterReplace.orderedParam(SELECT_3_FROM_0_1_WHERE_2,
        extraQuery.getIndexName(), extraQuery.getTypeName(), getFilterStr(extraQuery), "?0");
    ParameterizedString parameterizedString = new ParameterizedString(sql);
    Map<String, Object> res = new HashMap<>();
    for (int i = 0; i < select.length; i++) {
      res.put(extraQuery.getAs(i), parameterizedString);
      parameterizedString.nameToAlias(select[i], extraQuery.getAs(i));
    }
    return res;
  }

  private String getFilterStr(ExtraQuery extraQuery) {
    String s = extraQuery.getQueryBy().toString();
    return s.substring(1, s.length() - 1);
  }
}
