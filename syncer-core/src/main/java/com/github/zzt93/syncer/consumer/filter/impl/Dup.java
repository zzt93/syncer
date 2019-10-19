package com.github.zzt93.syncer.consumer.filter.impl;

import com.github.zzt93.syncer.common.data.MongoDataId;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.consumer.filter.ForkStatement;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class Dup implements ForkStatement {

  private final List<Statement> newObjAction;
  private final List<Expression> copyValue;


  public Dup(SpelExpressionParser parser, List<String> cp,
      ArrayList<List<String>> multiple) {
    copyValue = new ArrayList<>(cp.size());
    for (String s : cp) {
      copyValue.add(parser.parseExpression(s));
    }
    newObjAction = multiple.stream().map(action -> new Statement(parser, action)).collect(Collectors.toList());

  }

  public List<SyncData> execute(SyncData src) {
    LinkedList<SyncData> res = new LinkedList<>();
    for (int i = 0; i < newObjAction.size(); i++) {
      Statement statement = newObjAction.get(i);
      SyncData dup = new SyncData(src, MongoDataId.Offset.DUP.ordinal() + i);
      for (Expression s : copyValue) {
        Object value = s.getValue(src.getContext());
        s.setValue(dup.getContext(), value);
      }
      statement.execute(dup);
      res.add(dup);
    }
    return res;
  }

  @Override
  public void filter(List<SyncData> dataList) {
    List<SyncData> res = new LinkedList<>();
    for (SyncData syncData : dataList) {
      res.addAll(execute(syncData));
    }
    dataList.addAll(res);
  }
}
