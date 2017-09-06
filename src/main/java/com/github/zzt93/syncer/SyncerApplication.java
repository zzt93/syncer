package com.github.zzt93.syncer;

import com.github.zzt93.syncer.config.SyncerConfig;
import com.github.zzt93.syncer.config.input.Master;
import com.github.zzt93.syncer.input.connect.MasterConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;


@SpringBootApplication
public class SyncerApplication implements CommandLineRunner {

    @Autowired
    private SyncerConfig syncerConfig;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    public static void main(String[] args) {
        SpringApplication.run(SyncerApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        for (Master master : syncerConfig.getInput().getMasters()) {
            new MasterConnector(master).connect();
        }
    }

}
