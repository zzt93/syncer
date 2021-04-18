package com.github.zzt93.syncer.config.consumer.filter;

import com.github.zzt93.syncer.config.ConsumerConfig;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import com.github.zzt93.syncer.config.syncer.SyncerFilterMeta;
import com.github.zzt93.syncer.consumer.filter.impl.JavaMethod;
import com.github.zzt93.syncer.data.util.SyncFilter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author zzt
 */
@Setter
@Getter
@ConsumerConfig("filter")
public class FilterConfig {

  @ConsumerConfig("filter.sourcePath")
  private String sourcePath;

  /*---the following field is not configured---*/
  private SyncerFilterMeta filterMeta;
  private String consumerId;


  public SyncFilter toFilter() {
    if (sourcePath == null) {
      return null;
    }
    validate();
    Preconditions.checkState(filterMeta != null, "Not set filterMeta for method");
    return JavaMethod.build(getSourcePath());
  }

  private void validate() {
    Path path = Paths.get(sourcePath);
    if (!Files.exists(path)) {
      throw new InvalidConfigException("Fail to find " + sourcePath + ": Absolute path is better");
    }
    if (!sourcePath.endsWith(".java")) {
      throw new InvalidConfigException("Only support java source now: " + sourcePath);
    }
  }

  public FilterConfig addMeta(String consumerId, SyncerFilterMeta filterMeta) {
    this.filterMeta = filterMeta;
    this.consumerId = consumerId;
    return this;
  }

}
