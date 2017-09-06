package com.github.zzt93.syncer.config.input;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zzt
 */
public class Input {

    private List<Master> masters = new ArrayList<>();

    public List<Master> getMasters() {
        return masters;
    }

    public void setMasters(List<Master> masters) {
        this.masters = masters;
    }

}
