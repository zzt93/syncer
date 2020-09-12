package com.github.zzt93.syncer.config;

import ch.qos.logback.classic.Level;
import com.github.zzt93.syncer.SyncerApplication;
import com.github.zzt93.syncer.common.util.ArgUtil;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @author zzt
 */
public class CmdProcessor {

  private static final String DEBUG = "debug";

  public static SyncerApplication processCmdArgs(String[] args) {
    HashMap<String, String> argKV = ArgUtil.toMap(args);
    if (argKV.containsKey(DEBUG)) {
      ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
      root.setLevel(Level.DEBUG);
    }
    SyncerApplication syncerApplication = YamlEnvironmentPostProcessor.processEnvironment(argKV);
    SyncerConfig syncerConfig = syncerApplication.getSyncerConfig();
    if (argKV.containsKey(SyncerConfig.INSTANCE)) {
//      syncerConfig.setInstanceId(argKV.get(SyncerConfig.INSTANCE));
    }
    if (argKV.containsKey(SyncerConfig.SERVER_PORT)) {
      syncerConfig.setPort(Integer.parseInt(argKV.get(SyncerConfig.SERVER_PORT)));
    }

    return syncerApplication;
  }



}

