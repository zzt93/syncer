package com.github.zzt93.syncer.data;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public interface MethodFilter extends SyncFilter<SyncData> {

  Logger logger = LoggerFactory.getLogger(MethodFilter.class);

}
