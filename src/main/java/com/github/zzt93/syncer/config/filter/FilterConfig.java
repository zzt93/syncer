package com.github.zzt93.syncer.config.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zzt
 */
public class FilterConfig {

    private FilterType type;
    private String condition;
    private List<Map<String, List<String>>> action = new ArrayList<>();

    public FilterType getType() {
        return type;
    }

    public void setType(FilterType type) {
        this.type = type;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<Map<String, List<String>>> getAction() {
        return action;
    }

    public void setAction(List<Map<String, List<String>>> action) {
        this.action = action;
    }

    enum FilterType {
        IF
    }
}
