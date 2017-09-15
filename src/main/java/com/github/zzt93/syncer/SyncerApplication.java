package com.github.zzt93.syncer;

import com.github.zzt93.syncer.common.SyncData;
import com.github.zzt93.syncer.config.pipeline.PipelineConfig;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.filter.FilterStarter;
import com.github.zzt93.syncer.input.InputStarter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class SyncerApplication implements CommandLineRunner {

  @Autowired
  private PipelineConfig pipelineConfig;
  @Autowired
  private SyncerConfig syncerConfig;

  public static void main(String[] args) {
    SpringApplication.run(SyncerApplication.class, args);
  }

  @Override
  public void run(String... strings) throws Exception {
    BlockingQueue<SyncData> inputFilter = new LinkedBlockingQueue<>();
    BlockingQueue<SyncData> filterOutput = new LinkedBlockingQueue<>();
    InputStarter.getInstance(pipelineConfig.getInput(), syncerConfig.getInput(), inputFilter).start();
    FilterStarter.getInstance(pipelineConfig.getFilter(), syncerConfig.getFilter(), inputFilter, filterOutput).start();
  }

}
