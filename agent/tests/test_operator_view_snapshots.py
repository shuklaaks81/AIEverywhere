from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.graph import _build_operator_view, _render_operator_answer
from app.knowledge import query_knowledge

SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshots"


def _snapshot(name: str) -> dict:
    return json.loads((SNAPSHOT_DIR / f"{name}.json").read_text(encoding="utf-8"))


def _assert_snapshot(name: str, state: dict, summary_text: str) -> None:
    operator_view = _build_operator_view(state, summary_text)
    payload = {
        "status": operator_view["operator_status"],
        "summary": operator_view["operator_summary"],
        "sections": operator_view["answer_sections"],
        "alerts": operator_view["recommended_alerts"],
        "next_steps": operator_view["next_steps"],
        "answer": _render_operator_answer(operator_view["operator_summary"], operator_view["answer_sections"]),
    }
    assert payload == _snapshot(name)


def test_kafka_degraded_operator_view_snapshot() -> None:
    state = {
        "query_type": "kafka_status",
        "domain_data": {
            "status": "degraded",
            "summary": "Kafka synthetic metrics show attention points: 4 under-replicated partitions are reported; consumer lag is high at 1450",
            "details": {
                "brokers": 3,
                "consumer_lag": 1450,
                "under_replicated_partitions": 4,
            },
            "alert_recommendations": [
                {
                    "name": "KafkaUnderReplicatedPartitions",
                    "severity": "critical",
                    "condition": "Synthetic under-replicated partitions = 4.",
                    "rationale": "Replication safety is reduced and data loss risk increases if another broker fails.",
                },
                {
                    "name": "KafkaConsumerLagHigh",
                    "severity": "critical",
                    "condition": "Synthetic consumer lag = 1450.",
                    "rationale": "Consumers are materially behind and end-to-end latency is likely user visible.",
                },
            ],
            "remediation_playbook": [
                "Inspect broker health and ISR shrink events for the affected partitions.",
                "Confirm disk, CPU, and network are not throttling replication traffic.",
                "Delay partition reassignments or broker maintenance until replication catches up.",
            ],
        },
    }
    _assert_snapshot("kafka_degraded", state, "Kafka degraded")


def test_architecture_operator_view_snapshot() -> None:
    state = {
        "query_type": "architecture_info",
        "knowledge_data": query_knowledge("What is the architecture of this system?"),
    }
    _assert_snapshot("architecture_reference", state, "Architecture reference")


def test_change_log_operator_view_snapshot() -> None:
    state = {
        "query_type": "change_info",
        "knowledge_data": query_knowledge("What changed today?"),
    }
    _assert_snapshot("change_log_reference", state, "Change log reference")


def test_health_operator_view_snapshot() -> None:
    state = {
        "query_type": "overall_health",
        "metrics_data": {
            "summary": {
                "healthy": True,
                "targets": [
                    {"target": "prometheus", "status": "up", "instance": "prometheus:9090"},
                    {"target": "sample-service", "status": "up", "instance": "sample-service:8080"},
                    {"target": "nodeexporter", "status": "up", "instance": "nodeexporter:9100"},
                ],
                "down_targets": [],
                "summary": "All scraped targets are up.",
            }
        },
    }
    _assert_snapshot("health_healthy", state, "Health reference")


def test_performance_operator_view_snapshot() -> None:
    state = {
        "query_type": "performance_issue",
        "root_cause_data": {
            "summary": "slow_endpoint_latency_ms is 1200.0ms and considered high.; memory_usage is 88.0% and considered high.; Found 2 matching log lines in the last 10 minutes.",
            "findings": [
                "slow_endpoint_latency_ms is 1200.0ms and considered high.",
                "memory_usage is 88.0% and considered high.",
                "Found 2 matching log lines in the last 10 minutes.",
            ],
            "likely_causes": [
                "application request latency is elevated on the /slow path",
                "host memory pressure may be contributing",
                "recent logs contain slow, timeout, or failed-operation signals",
            ],
        },
    }
    _assert_snapshot("performance_degraded", state, "Performance reference")
