package com.github.zzt93.syncer.config.filter;

import java.util.List;

/**
 * Created by zzt on 9/11/17.
 * <p>
 * <h3></h3>
 */
public class If {

    private Condition condition;
    private List<Action> actions;

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public Condition getCondition() {

        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }
}
