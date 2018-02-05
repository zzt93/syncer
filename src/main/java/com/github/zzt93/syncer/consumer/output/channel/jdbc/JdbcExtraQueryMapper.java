package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.common.data.InsertByQuery;
import com.github.zzt93.syncer.common.expr.ParameterReplace;
import com.github.zzt93.syncer.consumer.output.channel.ExtraQueryMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author zzt
 */
@Deprecated
public class JdbcExtraQueryMapper implements ExtraQueryMapper {

  private static final String SELECT_3_FROM_0_1_WHERE_ID_2 = "select ?3 from `?0`.`?1` where ?2";
  private final Logger logger = LoggerFactory.getLogger(JdbcExtraQueryMapper.class);
  private final JdbcTemplate template;

  public JdbcExtraQueryMapper(JdbcTemplate jdbcTemplate) {
    template = jdbcTemplate;
  }

  @Override
  public Map<String, Object> map(InsertByQuery insertByQuery) {
    String[] target = insertByQuery.getSelect();
    String select = Arrays.toString(insertByQuery.getSelect());
    String sql = ParameterReplace.orderedParam(SELECT_3_FROM_0_1_WHERE_ID_2,
        insertByQuery.getIndexName(), insertByQuery.getTypeName(), insertByQuery.getQueryBy().toString(),
        select);
    List<Map<String, Object>> maps = template.queryForList(sql);
    if (maps.size() > 1) {
      logger.warn("Multiple query results exists, only use the first");
    } else if (maps.size() == 0) {
      logger.warn("Fail to find any match by " + insertByQuery);
      return Collections.emptyMap();
    }
    Map<String, Object> hit = maps.get(0);
    Map<String, Object> res = new HashMap<>();
    for (int i = 0; i < target.length; i++) {
      res.put(insertByQuery.getAs(i), hit.get(target[i]));
    }
    return res;
  }

}
