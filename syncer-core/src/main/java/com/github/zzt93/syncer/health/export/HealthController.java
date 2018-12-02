package com.github.zzt93.syncer.health.export;

import com.github.zzt93.syncer.health.SyncerHealth;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

  @GetMapping("/health")
  public String health() {
    return SyncerHealth.toJson();
  }

}
