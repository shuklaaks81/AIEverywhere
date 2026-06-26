package com.aieverywhere.sample;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DomainSignalsController {

    private static final Logger logger = LoggerFactory.getLogger(DomainSignalsController.class);

    private final AtomicInteger kafkaBrokers = new AtomicInteger(3);
    private final AtomicInteger kafkaConsumerLag = new AtomicInteger(12);
    private final AtomicInteger kafkaUnderReplicatedPartitions = new AtomicInteger(0);

    private final AtomicInteger igniteNodes = new AtomicInteger(2);
    private final AtomicReference<Double> igniteCacheHitRate = new AtomicReference<>(0.98);
    private final AtomicReference<Double> igniteMemoryPressure = new AtomicReference<>(38.0);
    private final AtomicInteger igniteRebalanceInProgress = new AtomicInteger(0);

    public DomainSignalsController(MeterRegistry meterRegistry) {
        Gauge.builder("aie.domain.kafka.brokers", kafkaBrokers, AtomicInteger::doubleValue).register(meterRegistry);
        Gauge.builder("aie.domain.kafka.consumer.lag", kafkaConsumerLag, AtomicInteger::doubleValue).register(meterRegistry);
        Gauge.builder("aie.domain.kafka.under.replicated.partitions", kafkaUnderReplicatedPartitions, AtomicInteger::doubleValue)
                .register(meterRegistry);

        Gauge.builder("aie.domain.ignite.nodes", igniteNodes, AtomicInteger::doubleValue).register(meterRegistry);
        Gauge.builder("aie.domain.ignite.cache.hit.rate", igniteCacheHitRate, value -> value.get()).register(meterRegistry);
        Gauge.builder("aie.domain.ignite.memory.pressure", igniteMemoryPressure, value -> value.get()).register(meterRegistry);
        Gauge.builder("aie.domain.ignite.rebalance.in.progress", igniteRebalanceInProgress, AtomicInteger::doubleValue)
                .register(meterRegistry);
    }

    @GetMapping("/demo/domain")
    public Map<String, Object> domainSignals() {
        return snapshot();
    }

    @PostMapping("/demo/domain/reset")
    public Map<String, Object> resetDomainSignals() {
        kafkaBrokers.set(3);
        kafkaConsumerLag.set(12);
        kafkaUnderReplicatedPartitions.set(0);

        igniteNodes.set(2);
        igniteCacheHitRate.set(0.98);
        igniteMemoryPressure.set(38.0);
        igniteRebalanceInProgress.set(0);

        logger.info("Reset synthetic Kafka and Ignite signals to the healthy demo baseline");
        return snapshot();
    }

    @PostMapping("/demo/domain/kafka")
    public Map<String, Object> updateKafkaSignals(
            @RequestParam(defaultValue = "3") int brokers,
            @RequestParam(defaultValue = "12") int consumerLag,
            @RequestParam(defaultValue = "0") int underReplicatedPartitions) {
        kafkaBrokers.set(Math.max(0, brokers));
        kafkaConsumerLag.set(Math.max(0, consumerLag));
        kafkaUnderReplicatedPartitions.set(Math.max(0, underReplicatedPartitions));

        logger.warn(
                "Updated synthetic Kafka signals brokers={} consumerLag={} underReplicatedPartitions={}",
                kafkaBrokers.get(),
                kafkaConsumerLag.get(),
                kafkaUnderReplicatedPartitions.get());
        return snapshot();
    }

    @PostMapping("/demo/domain/ignite")
    public Map<String, Object> updateIgniteSignals(
            @RequestParam(defaultValue = "2") int nodes,
            @RequestParam(defaultValue = "0.98") double cacheHitRate,
            @RequestParam(defaultValue = "38") double memoryPressure,
            @RequestParam(defaultValue = "0") int rebalanceInProgress) {
        igniteNodes.set(Math.max(0, nodes));
        igniteCacheHitRate.set(clamp(cacheHitRate, 0.0, 1.0));
        igniteMemoryPressure.set(clamp(memoryPressure, 0.0, 100.0));
        igniteRebalanceInProgress.set(rebalanceInProgress > 0 ? 1 : 0);

        logger.warn(
                "Updated synthetic Ignite signals nodes={} cacheHitRate={} memoryPressure={} rebalanceInProgress={}",
                igniteNodes.get(),
                igniteCacheHitRate.get(),
                igniteMemoryPressure.get(),
                igniteRebalanceInProgress.get());
        return snapshot();
    }

    private Map<String, Object> snapshot() {
        Map<String, Object> kafka = new LinkedHashMap<>();
        kafka.put("brokers", kafkaBrokers.get());
        kafka.put("consumerLag", kafkaConsumerLag.get());
        kafka.put("underReplicatedPartitions", kafkaUnderReplicatedPartitions.get());

        Map<String, Object> ignite = new LinkedHashMap<>();
        ignite.put("nodes", igniteNodes.get());
        ignite.put("cacheHitRate", igniteCacheHitRate.get());
        ignite.put("memoryPressure", igniteMemoryPressure.get());
        ignite.put("rebalanceInProgress", igniteRebalanceInProgress.get());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("kafka", kafka);
        response.put("ignite", ignite);
        return response;
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }
}