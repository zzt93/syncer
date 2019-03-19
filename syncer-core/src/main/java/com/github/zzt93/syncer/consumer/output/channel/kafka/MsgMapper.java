package com.github.zzt93.syncer.consumer.output.channel.kafka;

import com.github.zzt93.syncer.common.data.Mapper;
import com.github.zzt93.syncer.common.data.SyncData;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.consumer.output.kafka.MsgMapping;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zzt
 */
public class MsgMapper implements Mapper<SyncData, String> {

  private final Expression topic;
  private final boolean includeBefore;

  public MsgMapper(MsgMapping mapping) {
    includeBefore = mapping.isIncludeBefore();
    SpelExpressionParser parser = new SpelExpressionParser();
    try {
      topic = parser.parseExpression(mapping.getTopic());
    } catch (ParseException | IllegalStateException e) {
      throw new InvalidConfigException("Fail to parse [mapping] config for [redis] output", e);
    }
  }

  @Override
  public String map(SyncData syncData) {
    StandardEvaluationContext context = syncData.getContext();
    if (!includeBefore) {
      syncData.getResult().setBefore(null);
    }
    return topic.getValue(context, String.class);
  }
}
