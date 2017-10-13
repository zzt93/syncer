package com.github.zzt93.syncer.filter.impl;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.filter.CloneConfig;
import com.github.zzt93.syncer.filter.ExprFilter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

/**
 * @author zzt
 */
public class Clone implements ExprFilter {

  private final FilterActions newObjAction;
  private final FilterActions oldObjAction;
  private final SpelExpressionParser parser;
  private List<Field> fields = new ArrayList<>();

  public Clone(SpelExpressionParser parser,
      CloneConfig cloneConfig) throws NoSuchFieldException {
    this.parser = parser;
    for (String s : cloneConfig.getFields()) {
      fields.add(SyncData.class.getField(s));
    }
    newObjAction = new FilterActions(cloneConfig.getNew());
    oldObjAction = new FilterActions(cloneConfig.getOld());
  }

  @Override
  public Void decide(List<SyncData> dataList) {
    LinkedList<SyncData> list = new LinkedList<>();
    for (SyncData syncData : dataList) {
      SyncData clone = new SyncData(syncData.getType());
      for (Field field : fields) {
        Object fieldValue = ReflectionUtils.getField(field, syncData);
        ReflectionUtils.setField(field, clone, fieldValue);
      }
      newObjAction.execute(parser, clone.getContext());
      oldObjAction.execute(parser, syncData.getContext());
      list.add(clone);
    }
    dataList.addAll(list);
    return null;
  }
}
