package com.github.zzt93.syncer.health.export;

import com.github.zzt93.syncer.health.Health;

/**
 * @author zzt
 */
public class ExportResult {
  private final String json;
  private final Health.HealthStatus overall;

  public ExportResult(String json, Health.HealthStatus overall) {
    this.json = json;
    this.overall = overall;
  }

  public String getJson() {
    return json;
  }

  public Health.HealthStatus getOverall() {
    return overall;
  }
}
