package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.ExtraQuery;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.consumer.output.channel.ExtraQueryMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class JdbcNestedQueryMapper implements ExtraQueryMapper {

  private static final String SELECT_3_FROM_0_1_WHERE_2 = "select ?3 from `?0`.`?1` where ?2";

  @Override
  public Map<String, Object> map(ExtraQuery extraQuery) {
    String[] select = extraQuery.getSelect();
    String sql = ParameterReplace.orderedParam(SELECT_3_FROM_0_1_WHERE_2,
        extraQuery.getIndexName(), extraQuery.getTypeName(),
        getFilterStr(extraQuery), "?0");
    ParameterizedString parameterizedString = new ParameterizedString(sql);
    Map<String, Object> res = new HashMap<>();
    for (int i = 0; i < select.length; i++) {
      res.put(extraQuery.getAs(i), parameterizedString);
      parameterizedString.nameToAlias(select[i], extraQuery.getAs(i));
    }
    return res;
  }

  private String getFilterStr(ExtraQuery extraQuery) {
    String prefix = extraQuery.getIndexName() + "." + extraQuery.getTypeName() + ".";
    return extraQuery.getQueryBy().entrySet().stream().map(e -> prefix + e.toString())
        .collect(Collectors.joining(", "));
  }
}
