package com.github.zzt93.syncer.output;

import com.github.zzt93.syncer.common.SyncData;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author zzt
 */
@Component
@ConditionalOnProperty(prefix = "syncer.output.http.connection", name = {"host", "port"})
public class HttpChannel implements OutputChannel {
    @Override
    public boolean output(SyncData event) {
        return false;
    }

    @Override
    public boolean output(List<SyncData> batch) {
        return false;
    }
}
