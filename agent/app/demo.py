from __future__ import annotations

from copy import deepcopy

from app.schemas import QueryResponse


_DEMO_RESPONSES: dict[str, dict] = {
    "health": {
        "answer": "Summary: All scraped targets are up.\n\nTargets:\n- prometheus: up\n- sample-service: up\n- nodeexporter: up",
        "status": "healthy",
        "summary": "All scraped targets are up.",
        "sections": [
            {"title": "Targets", "items": ["prometheus: up", "sample-service: up", "nodeexporter: up"]},
            {"title": "Next Steps", "items": []},
        ],
        "alerts": [],
        "next_steps": [],
        "query_type": "overall_health",
        "conversation_id": "demo-health",
        "used_sources": ["demo:fixture"],
        "context": {
            "metrics_data": {
                "query": "up",
                "kind": "overall_health",
                "raw_status": "success",
                "summary": {
                    "healthy": True,
                    "targets": [
                        {"target": "prometheus", "status": "up", "instance": "prometheus:9090"},
                        {"target": "sample-service", "status": "up", "instance": "sample-service:8080"},
                        {"target": "nodeexporter", "status": "up", "instance": "nodeexporter:9100"},
                    ],
                    "down_targets": [],
                    "summary": "All scraped targets are up.",
                },
            },
            "logs_data": {},
            "root_cause_data": {},
            "knowledge_data": {},
            "domain_data": {},
        },
    },
    "architecture": {
        "answer": "Summary: Local architecture reference retrieved with runtime model and planned component context.\n\nCore Components:\n- Grafana OSS provides dashboards and log exploration.\n- Prometheus scrapes metrics from Prometheus itself, nodeexporter, and the sample Spring Boot service.\n- Loki stores logs and is fed by Promtail.\n- Ollama provides local offline LLM inference for the agent.\n- The agent is a FastAPI and LangGraph service that answers observability questions.\n- The sample Spring Boot service exposes Actuator and Prometheus metrics.\n\nRuntime Model:\n- Primary model: qwen2.5-coder:14b\n- Fallback model: deepseek-coder:6.7b\n\nPlanned Components:\n- Kafka is modeled in Day 5 through synthetic metrics exported by the sample Spring Boot service.\n- Apache Ignite is modeled in Day 5 through synthetic metrics exported by the sample Spring Boot service.\n- In the current repo state, Kafka and Ignite are not yet running containers.\n- The agent reads those simulated component signals from Prometheus so domain questions are grounded in live telemetry.\n\nReasoning Model:\n- Day 2: single-turn health and log inspection.\n- Day 3: conversation memory and basic root-cause heuristics.\n- Day 4 target: domain-specific knowledge and component-aware questions.\n- Day 5 target: synthetic Kafka and Ignite signal ingestion via Prometheus-backed domain metrics.\n- Day 6 target: alert recommendations and remediation playbooks derived from domain signal thresholds.\n\nSource:\n- architecture.md",
        "status": "reference",
        "summary": "Local architecture reference retrieved with runtime model and planned component context.",
        "sections": [
            {"title": "Core Components", "items": [
                "Grafana OSS provides dashboards and log exploration.",
                "Prometheus scrapes metrics from Prometheus itself, nodeexporter, and the sample Spring Boot service.",
                "Loki stores logs and is fed by Promtail.",
                "Ollama provides local offline LLM inference for the agent.",
                "The agent is a FastAPI and LangGraph service that answers observability questions.",
                "The sample Spring Boot service exposes Actuator and Prometheus metrics."]},
            {"title": "Runtime Model", "items": ["Primary model: qwen2.5-coder:14b", "Fallback model: deepseek-coder:6.7b"]},
            {"title": "Planned Components", "items": [
                "Kafka is modeled in Day 5 through synthetic metrics exported by the sample Spring Boot service.",
                "Apache Ignite is modeled in Day 5 through synthetic metrics exported by the sample Spring Boot service.",
                "In the current repo state, Kafka and Ignite are not yet running containers.",
                "The agent reads those simulated component signals from Prometheus so domain questions are grounded in live telemetry."]},
            {"title": "Reasoning Model", "items": [
                "Day 2: single-turn health and log inspection.",
                "Day 3: conversation memory and basic root-cause heuristics.",
                "Day 4 target: domain-specific knowledge and component-aware questions.",
                "Day 5 target: synthetic Kafka and Ignite signal ingestion via Prometheus-backed domain metrics.",
                "Day 6 target: alert recommendations and remediation playbooks derived from domain signal thresholds."]},
            {"title": "Source", "items": ["architecture.md"]},
        ],
        "alerts": [],
        "next_steps": [],
        "query_type": "architecture_info",
        "conversation_id": "demo-architecture",
        "used_sources": ["demo:fixture"],
        "context": {
            "metrics_data": {},
            "logs_data": {},
            "root_cause_data": {},
            "knowledge_data": {"source": "architecture.md"},
            "domain_data": {},
        },
    },
    "kafka": {
        "answer": "Summary: Kafka synthetic metrics show attention points: 4 under-replicated partitions are reported; consumer lag is high at 1450\n\nSignals:\n- Status: degraded\n- Brokers: 3\n- Consumer lag: 1450\n- Under-replicated partitions: 4\n\nAlerts:\n- KafkaUnderReplicatedPartitions (critical): Synthetic under-replicated partitions = 4.\n- KafkaConsumerLagHigh (critical): Synthetic consumer lag = 1450.\n\nNext Steps:\n- Inspect broker health and ISR shrink events for the affected partitions.\n- Confirm disk, CPU, and network are not throttling replication traffic.\n- Delay partition reassignments or broker maintenance until replication catches up.",
        "status": "degraded",
        "summary": "Kafka synthetic metrics show attention points: 4 under-replicated partitions are reported; consumer lag is high at 1450",
        "sections": [
            {"title": "Signals", "items": ["Status: degraded", "Brokers: 3", "Consumer lag: 1450", "Under-replicated partitions: 4"]},
            {"title": "Alerts", "items": ["KafkaUnderReplicatedPartitions (critical): Synthetic under-replicated partitions = 4.", "KafkaConsumerLagHigh (critical): Synthetic consumer lag = 1450."]},
            {"title": "Next Steps", "items": ["Inspect broker health and ISR shrink events for the affected partitions.", "Confirm disk, CPU, and network are not throttling replication traffic.", "Delay partition reassignments or broker maintenance until replication catches up."]},
        ],
        "alerts": [
            {"name": "KafkaUnderReplicatedPartitions", "severity": "critical", "condition": "Synthetic under-replicated partitions = 4.", "rationale": "Replication safety is reduced and data loss risk increases if another broker fails."},
            {"name": "KafkaConsumerLagHigh", "severity": "critical", "condition": "Synthetic consumer lag = 1450.", "rationale": "Consumers are materially behind and end-to-end latency is likely user visible."},
        ],
        "next_steps": [
            "Inspect broker health and ISR shrink events for the affected partitions.",
            "Confirm disk, CPU, and network are not throttling replication traffic.",
            "Delay partition reassignments or broker maintenance until replication catches up.",
        ],
        "query_type": "kafka_status",
        "conversation_id": "demo-kafka",
        "used_sources": ["demo:fixture"],
        "context": {"metrics_data": {}, "logs_data": {}, "root_cause_data": {}, "knowledge_data": {}, "domain_data": {"status": "degraded"}},
    },
    "ignite": {
        "answer": "Summary: Ignite synthetic metrics show attention points: memory pressure is high at 88.0%; cache hit rate is low at 84.0%; rebalance is currently in progress\n\nSignals:\n- Status: degraded\n- Cluster nodes: 2\n- Cache hit rate: 0.84\n- Memory pressure: 88.0%\n- Rebalance in progress: True\n\nAlerts:\n- IgniteMemoryPressureHigh (critical): Synthetic Ignite memory pressure = 88.0%.\n- IgniteCacheHitRateLow (warning): Synthetic cache hit rate = 84.0%.\n- IgniteRebalanceInProgress (info): Synthetic Ignite rebalance flag is active.\n\nNext Steps:\n- Reduce hot cache footprint or add capacity before the cluster reaches an eviction spiral.\n- Inspect large keys, near-cache growth, and recent workload spikes that changed memory shape.\n- Validate JVM and off-heap memory sizing assumptions against current traffic.",
        "status": "degraded",
        "summary": "Ignite synthetic metrics show attention points: memory pressure is high at 88.0%; cache hit rate is low at 84.0%; rebalance is currently in progress",
        "sections": [
            {"title": "Signals", "items": ["Status: degraded", "Cluster nodes: 2", "Cache hit rate: 0.84", "Memory pressure: 88.0%", "Rebalance in progress: True"]},
            {"title": "Alerts", "items": ["IgniteMemoryPressureHigh (critical): Synthetic Ignite memory pressure = 88.0%.", "IgniteCacheHitRateLow (warning): Synthetic cache hit rate = 84.0%.", "IgniteRebalanceInProgress (info): Synthetic Ignite rebalance flag is active."]},
            {"title": "Next Steps", "items": ["Reduce hot cache footprint or add capacity before the cluster reaches an eviction spiral.", "Inspect large keys, near-cache growth, and recent workload spikes that changed memory shape.", "Validate JVM and off-heap memory sizing assumptions against current traffic."]},
        ],
        "alerts": [
            {"name": "IgniteMemoryPressureHigh", "severity": "critical", "condition": "Synthetic Ignite memory pressure = 88.0%.", "rationale": "The data grid is close to eviction, GC churn, or node instability."},
            {"name": "IgniteCacheHitRateLow", "severity": "warning", "condition": "Synthetic cache hit rate = 84.0%.", "rationale": "Cache misses are high enough to push more load onto backing services."},
            {"name": "IgniteRebalanceInProgress", "severity": "info", "condition": "Synthetic Ignite rebalance flag is active.", "rationale": "Rebalance can temporarily amplify latency or memory pressure during topology change."},
        ],
        "next_steps": [
            "Reduce hot cache footprint or add capacity before the cluster reaches an eviction spiral.",
            "Inspect large keys, near-cache growth, and recent workload spikes that changed memory shape.",
            "Validate JVM and off-heap memory sizing assumptions against current traffic.",
        ],
        "query_type": "ignite_status",
        "conversation_id": "demo-ignite",
        "used_sources": ["demo:fixture"],
        "context": {"metrics_data": {}, "logs_data": {}, "root_cause_data": {}, "knowledge_data": {}, "domain_data": {"status": "degraded"}},
    },
    "performance": {
        "answer": "Summary: slow_endpoint_latency_ms is 1200.0ms and considered high.; memory_usage is 88.0% and considered high.; Found 2 matching log lines in the last 10 minutes.\n\nFindings:\n- slow_endpoint_latency_ms is 1200.0ms and considered high.\n- memory_usage is 88.0% and considered high.\n- Found 2 matching log lines in the last 10 minutes.\n\nLikely Causes:\n- application request latency is elevated on the /slow path\n- host memory pressure may be contributing\n- recent logs contain slow, timeout, or failed-operation signals\n\nNext Steps:\n- Check whether application request latency is elevated on the /slow path.\n- Check whether host memory pressure may be contributing.\n- Check whether recent logs contain slow, timeout, or failed-operation signals.",
        "status": "degraded",
        "summary": "slow_endpoint_latency_ms is 1200.0ms and considered high.; memory_usage is 88.0% and considered high.; Found 2 matching log lines in the last 10 minutes.",
        "sections": [
            {"title": "Findings", "items": ["slow_endpoint_latency_ms is 1200.0ms and considered high.", "memory_usage is 88.0% and considered high.", "Found 2 matching log lines in the last 10 minutes."]},
            {"title": "Likely Causes", "items": ["application request latency is elevated on the /slow path", "host memory pressure may be contributing", "recent logs contain slow, timeout, or failed-operation signals"]},
            {"title": "Next Steps", "items": ["Check whether application request latency is elevated on the /slow path.", "Check whether host memory pressure may be contributing.", "Check whether recent logs contain slow, timeout, or failed-operation signals."]},
        ],
        "alerts": [],
        "next_steps": [
            "Check whether application request latency is elevated on the /slow path.",
            "Check whether host memory pressure may be contributing.",
            "Check whether recent logs contain slow, timeout, or failed-operation signals.",
        ],
        "query_type": "performance_issue",
        "conversation_id": "demo-performance",
        "used_sources": ["demo:fixture"],
        "context": {"metrics_data": {}, "logs_data": {}, "root_cause_data": {"summary": "demo"}, "knowledge_data": {}, "domain_data": {}},
    },
    "change": {
        "answer": "Summary: Recent recorded project changes retrieved from the local change log.\n\nDay 5 Status:\n- Added synthetic Kafka and Ignite metrics to the sample service for Prometheus ingestion.\n- Switched Kafka and Ignite agent answers from static placeholders to Prometheus-backed synthetic telemetry.\n\nDay 6 Status:\n- Added alert recommendations for Kafka and Ignite based on synthetic domain thresholds.\n- Added remediation playbook steps so domain answers now include actionable next steps.\n\nDay 7 Status:\n- Polished operator UX with concise structured answers built from Summary, section blocks, alerts, and next steps.\n- Added top-level structured response fields so UI clients can render operator guidance without parsing freeform text.\n\nSource:\n- change-log.md",
        "status": "reference",
        "summary": "Recent recorded project changes retrieved from the local change log.",
        "sections": [
            {"title": "Day 5 Status", "items": [
                "Added synthetic Kafka and Ignite metrics to the sample service for Prometheus ingestion.",
                "Switched Kafka and Ignite agent answers from static placeholders to Prometheus-backed synthetic telemetry."]},
            {"title": "Day 6 Status", "items": [
                "Added alert recommendations for Kafka and Ignite based on synthetic domain thresholds.",
                "Added remediation playbook steps so domain answers now include actionable next steps."]},
            {"title": "Day 7 Status", "items": [
                "Polished operator UX with concise structured answers built from Summary, section blocks, alerts, and next steps.",
                "Added top-level structured response fields so UI clients can render operator guidance without parsing freeform text."]},
            {"title": "Source", "items": ["change-log.md"]},
        ],
        "alerts": [],
        "next_steps": [],
        "query_type": "change_info",
        "conversation_id": "demo-change",
        "used_sources": ["demo:fixture"],
        "context": {"metrics_data": {}, "logs_data": {}, "root_cause_data": {}, "knowledge_data": {"source": "change-log.md"}, "domain_data": {}},
    },
    "logs": {
        "answer": "Summary: Found 3 matching log lines in the last 10 minutes.\n\nExamples:\n- level=error service=sample-service message=Timeout calling downstream dependency\n- level=error service=sample-service message=Retry budget exhausted for ping workflow\n- java.lang.IllegalStateException: synthetic demo exception\n\nNext Steps:\n- Inspect the matching service logs in Grafana or Loki for full context.",
        "status": "attention",
        "summary": "Found 3 matching log lines in the last 10 minutes.",
        "sections": [
            {"title": "Examples", "items": [
                "level=error service=sample-service message=Timeout calling downstream dependency",
                "level=error service=sample-service message=Retry budget exhausted for ping workflow",
                "java.lang.IllegalStateException: synthetic demo exception"]},
            {"title": "Next Steps", "items": ["Inspect the matching service logs in Grafana or Loki for full context."]},
        ],
        "alerts": [],
        "next_steps": ["Inspect the matching service logs in Grafana or Loki for full context."],
        "query_type": "log_inquiry",
        "conversation_id": "demo-logs",
        "used_sources": ["demo:fixture"],
        "context": {
            "metrics_data": {},
            "logs_data": {
                "query": "{job=\"docker\"} |~ \"(?i)error|exception\"",
                "line_count": 3,
                "examples": [
                    "level=error service=sample-service message=Timeout calling downstream dependency",
                    "level=error service=sample-service message=Retry budget exhausted for ping workflow",
                    "java.lang.IllegalStateException: synthetic demo exception"
                ],
                "summary": "Found 3 matching log lines in the last 10 minutes."
            },
            "root_cause_data": {},
            "knowledge_data": {},
            "domain_data": {}
        },
    },
}


SCENARIO_ALIASES = {
    "system-health": "health",
    "operator-console": "kafka",
    "response-view": "kafka",
    "change_info": "change",
    "log_inquiry": "logs",
}


def list_demo_scenarios() -> list[str]:
    return sorted(_DEMO_RESPONSES.keys())



def build_demo_response(scenario: str) -> QueryResponse:
    scenario_key = SCENARIO_ALIASES.get(scenario, scenario)
    if scenario_key not in _DEMO_RESPONSES:
        raise KeyError(scenario)
    return QueryResponse(**deepcopy(_DEMO_RESPONSES[scenario_key]))
