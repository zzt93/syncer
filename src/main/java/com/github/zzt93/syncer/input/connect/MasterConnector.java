package com.github.zzt93.syncer;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.zzt93.syncer.config.InvalidPasswordException;
import com.github.zzt93.syncer.config.input.Master;
import com.github.zzt93.syncer.input.connect.NamedJob;
import com.github.zzt93.syncer.input.connect.NamedThreadFactory;
import com.github.zzt93.syncer.input.listener.LogLifecycleListener;
import com.github.zzt93.syncer.input.listener.SyncListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zzt
 */
public class MasterConnector {

    private static final ExecutorService service = Executors.newFixedThreadPool(4, new NamedThreadFactory());
    private final String remote;
    private Logger logger = LoggerFactory.getLogger(MasterConnector.class);
    private BinaryLogClient client;

    public MasterConnector(Master master) throws IOException {
        String password = FileCopyUtils.copyToString(new InputStreamReader(ClassLoader.getSystemResourceAsStream(master.getPasswordFile())));
        if (StringUtils.isEmpty(password)) {
            throw new InvalidPasswordException(password);
        }
        client = new BinaryLogClient(master.getAddress(), master.getPort(), master.getUser(), password);
        client.registerEventListener(new SyncListener());
        client.registerLifecycleListener(new LogLifecycleListener());

        this.remote = master.getAddress() + ":" + master.getPort();
    }

    public void connect() throws IOException {
        service.submit(new NamedJob(remote) {
            @Override
            public void run() {
                    try {
                        client.connect();
                    } catch (IOException e) {
                        logger.error("Fail to connect to master", e);
                    }
            }
        });
    }
}
