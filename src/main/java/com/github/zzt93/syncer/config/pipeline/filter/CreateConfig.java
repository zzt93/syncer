package com.github.zzt93.syncer.config.pipeline.filter;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.filter.impl.Create;
import com.github.zzt93.syncer.consumer.filter.impl.Dup;
import com.github.zzt93.syncer.consumer.filter.impl.IfBodyAction;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.springframework.expression.spel.standard.SpelExpressionParser;

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

  public IfBodyAction toAction(SpelExpressionParser parser) throws NoSuchFieldException {
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
