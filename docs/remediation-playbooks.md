# Remediation Playbooks

## Kafka Domain Signals

### KafkaBrokersDown

- Restore at least one reachable broker before resuming risky producer or consumer changes.
- Validate listeners, advertised listeners, and network reachability.
- Check recent broker restarts, host pressure, and storage availability.

### KafkaUnderReplicatedPartitions

- Inspect ISR shrink events and broker health for the affected partitions.
- Check disk, CPU, and network pressure that may slow replication.
- Postpone maintenance or partition movement until replicas recover.

### KafkaConsumerLagHigh

- Identify lagging consumer groups and verify offset commit health.
- Scale or restart consumers only after confirming safe replay behavior.
- Look for downstream dependency slowdowns or recent code changes.

## Ignite Domain Signals

### IgniteClusterUnavailable

- Recover at least one node and verify discovery or baseline topology.
- Check persistence, restarts, and network reachability between members.
- Reduce cache-dependent load if the application has a safe degraded mode.

### IgniteMemoryPressureHigh

- Reduce cache footprint or add capacity before eviction pressure worsens.
- Inspect hot keys, near-cache growth, and recent workload spikes.
- Re-check JVM and off-heap sizing assumptions.

### IgniteCacheHitRateLow

- Check for unexpected key expiration or access-pattern changes.
- Warm critical keys and review TTL policies for frequently reused data.
- Confirm backing services can absorb extra cache misses while tuning.

### IgniteRebalanceInProgress

- Avoid additional topology changes during rebalance.
- Watch latency and memory during partition movement.
- Delay heavy batch work until rebalance completes.
