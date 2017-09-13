package com.github.zzt93.syncer.input.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.zzt93.syncer.config.InvalidPasswordException;
import com.github.zzt93.syncer.config.common.MetaData;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.input.Schema;
import com.github.zzt93.syncer.filter.EventToSyncData;
import com.github.zzt93.syncer.filter.Filter;
import com.github.zzt93.syncer.filter.SchemaFilter;
import com.github.zzt93.syncer.input.listener.LogLifecycleListener;
import com.github.zzt93.syncer.input.listener.SyncListener;
import com.github.zzt93.syncer.util.FileUtil;
import com.github.zzt93.syncer.util.NetworkUtil;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * @author zzt
 */
public class MasterConnector {

    private static final ExecutorService service = Executors.newFixedThreadPool(4, new NamedThreadFactory());
    private final String remote;
    private Logger logger = LoggerFactory.getLogger(MasterConnector.class);
    private BinaryLogClient client;

    public MasterConnector(MysqlConnection connection, Schema schema) throws IOException {
        String password = FileUtil.readAll(connection.getPasswordFile());
        if (StringUtils.isEmpty(password)) {
            throw new InvalidPasswordException(password);
        }
        client = new BinaryLogClient(connection.getAddress(), connection.getPort(), connection.getUser(), password);
        client.registerLifecycleListener(new LogLifecycleListener());

        List<Filter> filters = new ArrayList<>();
        if (schema != null) {
            try {
                MetaData metaData = new MetaData.MetaDataBuilder(connection, schema).build();
                filters.add(new SchemaFilter(metaData, schema));
            } catch (SQLException e) {
                logger.error("Fail to connect to master to retrive metadata", e);
            }
        }
        filters.add(new EventToSyncData());
        client.registerEventListener(new SyncListener(filters));

        remote = NetworkUtil.toIp(connection.getAddress()) + ":" + connection.getPort();

    }

    public void connect() throws IOException {
        service.submit(() -> {
            Thread.currentThread().setName(remote);
            try {
                client.connect();
            } catch (IOException e) {
                logger.error("Fail to connect to master", e);
            }
        });
    }
}
