package com.github.zzt93.syncer.config.consumer.filter;

import com.github.zzt93.syncer.config.consumer.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.filter.ForkStatement;
import com.github.zzt93.syncer.consumer.filter.impl.Create;
import com.github.zzt93.syncer.consumer.filter.impl.Dup;
import com.google.common.collect.Lists;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class CreateConfig {

  /**
   * default only retain event meta info, i.e. non-data field
   * @see com.github.zzt93.syncer.common.data.SyncData.Meta
   */
  private List<String> copy = Lists.newArrayList();
  private List<Object> postCreation = new ArrayList<>();

  public List<String> getCopy() {
    return copy;
  }

  public void setCopy(List<String> copy) {
    this.copy = copy;
  }

  public List<Object> getPostCreation() {
    return postCreation;
  }

  public void setPostCreation(List<Object> postCreation) {
    this.postCreation = postCreation;
  }

  ForkStatement toAction(SpelExpressionParser parser) throws NoSuchFieldException {
    ArrayList<String> single = new ArrayList<>(postCreation.size());
    ArrayList<List<String>> multiple = new ArrayList<>(postCreation.size());
    for (Object o : postCreation) {
      if (o instanceof String) {
        single.add((String) o);
      } else if (o instanceof List) {
        multiple.add((List<String>) o);
      } else {
        throw new InvalidConfigException();
      }
    }
    if (single.isEmpty() && !multiple.isEmpty()) {
      return new Dup(parser, copy, multiple);
    } else if (multiple.isEmpty() && !single.isEmpty()) {
      return new Create(parser, copy, single);
    }
    throw new InvalidConfigException();
  }

}
