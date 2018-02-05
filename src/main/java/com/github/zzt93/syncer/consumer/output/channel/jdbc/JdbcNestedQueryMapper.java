package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.InsertByQuery;
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
  public Map<String, Object> map(InsertByQuery insertByQuery) {
    String[] select = insertByQuery.getSelect();
    String sql = ParameterReplace.orderedParam(SELECT_3_FROM_0_1_WHERE_2,
        insertByQuery.getIndexName(), insertByQuery.getTypeName(), getFilterStr(insertByQuery), "?0");
    ParameterizedString parameterizedString = new ParameterizedString(sql);
    Map<String, Object> res = new HashMap<>();
    for (int i = 0; i < select.length; i++) {
      res.put(insertByQuery.getAs(i), parameterizedString);
      parameterizedString.nameToAlias(select[i], insertByQuery.getAs(i));
    }
    return res;
  }

  private String getFilterStr(InsertByQuery insertByQuery) {
    String s = insertByQuery.getQueryBy().toString();
    return s.substring(1, s.length() - 1);
  }
}
