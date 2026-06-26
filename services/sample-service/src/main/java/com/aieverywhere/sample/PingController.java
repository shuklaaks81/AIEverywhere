package com.aieverywhere.sample;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingController {

    private static final Logger logger = LoggerFactory.getLogger(PingController.class);

    @GetMapping("/ping")
    public Map<String, String> ping() {
        logger.info("Ping request received at {}", Instant.now());
        return Map.of(
                "status", "ok",
                "service", "sample-service",
                "timestamp", Instant.now().toString());
    }

    @GetMapping("/slow")
    public Map<String, String> slow(@RequestParam(defaultValue = "900") long delayMs) throws InterruptedException {
        logger.warn("sample-service slow request simulated with delayMs={}", delayMs);
        TimeUnit.MILLISECONDS.sleep(delayMs);
        return Map.of(
                "status", "ok",
                "service", "sample-service",
                "delayMs", Long.toString(delayMs),
                "timestamp", Instant.now().toString());
    }
}
