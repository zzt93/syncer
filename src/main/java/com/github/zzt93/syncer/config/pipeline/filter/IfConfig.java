package com.github.zzt93.syncer.config.pipeline.filter;

import com.github.zzt93.syncer.config.pipeline.common.InvalidConfigException;
import com.github.zzt93.syncer.consumer.filter.impl.Drop;
import com.github.zzt93.syncer.consumer.filter.impl.ForeachFilter;
import com.github.zzt93.syncer.consumer.filter.impl.IfBodyAction;
import com.github.zzt93.syncer.consumer.filter.impl.Statement;
import com.github.zzt93.syncer.consumer.filter.impl.Switch;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author zzt
 */
public class IfConfig {

  private String condition;
  private List<IfStatement> ifBody;
  private List<IfStatement> elseBody;

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public List<IfStatement> getIfBody() {
    return ifBody;
  }

  public void setIfBody(List<IfStatement> ifBody) {
    this.ifBody = ifBody;
  }

  public List<IfBodyAction> getIfAction(
      SpelExpressionParser parser) {
    return getBodyAction(ifBody, parser);
  }

  public List<IfBodyAction> getElseAction(
      SpelExpressionParser parser) {
    return getBodyAction(elseBody, parser);
  }

  private List<IfBodyAction> getBodyAction(List<IfStatement> body,
      SpelExpressionParser parser) {
    if (body == null) {
      return Collections.emptyList();
    }
    List<IfBodyAction> res = new ArrayList<>();
    for (IfStatement statement : body) {
      if (statement.getCreate() != null) {
        try {
          res.add(statement.getCreate().toAction(parser));
        } catch (NoSuchFieldException e) {
          throw new InvalidConfigException("Unknown field of `SyncData` to copy", e);
        }
      } else if (statement.getDrop() != null) {
        res.add(new Drop());
      } else if (statement.getStatement() != null) {
        res.add(new Statement(parser, statement.getStatement()));
      } else if (statement.getSwitcher() != null) {
        res.add(new Switch(parser, statement.getSwitcher()));
      } else if (statement.getForeach() !=null) {
        res.add(new ForeachFilter(parser, statement.getForeach()));
      } else {
        Preconditions.checkState(false);
      }
    }
    return res;
  }

  public List<IfStatement> getElseBody() {
    return elseBody;
  }

  public void setElseBody(List<IfStatement> elseBody) {
    this.elseBody = elseBody;
  }

  public void checkConfig() {
    if (getCondition() == null) {
      throw new InvalidConfigException("Lack `condition` in `if`, which is a must");
    }
    if (getIfBody() == null) {
      throw new InvalidConfigException("Lack `if-body` in `if`, which is a must");
    }
  }

  public static class IfStatement {

    private CreateConfig create;
    private List<String> statement;
    private Map drop;
    private Switcher switcher;
    private ForeachConfig foreach;

    public ForeachConfig getForeach() {
      return foreach;
    }

    public void setForeach(ForeachConfig foreach) {
      this.foreach = foreach;
    }

    Switcher getSwitcher() {
      return switcher;
    }

    public void setSwitcher(Switcher switcher) {
      this.switcher = switcher;
    }

    CreateConfig getCreate() {
      return create;
    }

    public void setCreate(CreateConfig create) {
      this.create = create;
    }

    public List<String> getStatement() {
      return statement;
    }

    public void setStatement(List<String> statement) {
      this.statement = statement;
    }

    Map getDrop() {
      return drop;
    }

    public void setDrop(Map drop) {
      this.drop = drop;
    }

}

}
