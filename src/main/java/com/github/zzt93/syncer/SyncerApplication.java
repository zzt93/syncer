package com.github.zzt93.syncer;

import com.github.zzt93.syncer.config.ConfigHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


@SpringBootApplication
@EnableConfigurationProperties(ConfigHelper.class)
public class SyncerApplication implements CommandLineRunner {

    @Autowired
    private ConfigHelper helper;

    public static void main(String[] args) {
        SpringApplication.run(SyncerApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        for (ConfigHelper.Master master : helper.getMasters()) {
            new MasterConnector(master).connect();
        }
    }

}
