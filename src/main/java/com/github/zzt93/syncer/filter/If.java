package com.github.zzt93.syncer.filter;

import com.github.zzt93.syncer.common.SyncData;
import java.util.List;

/**
 * Created by zzt on 9/11/17.
 * <p>
 * <h3></h3>
 */
public class If implements Filter<SyncData, SyncData> {

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

    @Override
    public SyncData decide(SyncData data) {

        return data;
    }
}
