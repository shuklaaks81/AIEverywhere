from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any

import httpx

from app.knowledge import query_knowledge
from app.config import settings


def _prometheus_scalar(promql: str) -> float | None:
    payload = _prometheus_query(promql)
    result = payload.get("data", {}).get("result", [])
    if not result:
        return None
    raw_value = result[0].get("value", [None, None])[1]
    if raw_value is None:
        return None
    return float(raw_value)


def _prometheus_query(promql: str) -> dict[str, Any]:
    with httpx.Client(timeout=settings.metrics_timeout_seconds) as client:
        response = client.get(f"{settings.prometheus_url}/api/v1/query", params={"query": promql})
        response.raise_for_status()
        return response.json()


def _loki_query(logql: str, minutes: int = 10, limit: int = 20) -> dict[str, Any]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    params = {
        "query": logql,
        "limit": str(limit),
        "direction": "backward",
        "start": str(int(start.timestamp() * 1_000_000_000)),
        "end": str(int(end.timestamp() * 1_000_000_000)),
    }
    with httpx.Client(timeout=settings.logs_timeout_seconds) as client:
        response = client.get(f"{settings.loki_url}/loki/api/v1/query_range", params=params)
        response.raise_for_status()
        return response.json()


def _service_metrics_text() -> str:
    with httpx.Client(timeout=settings.metrics_timeout_seconds) as client:
        response = client.get(f"{settings.sample_service_url}/actuator/prometheus")
        response.raise_for_status()
        return response.text


def classify_query_type(question: str) -> str:
    lowered = question.lower()
    if any(token in lowered for token in ["kafka", "broker", "consumer lag", "partition"]):
        return "kafka_status"
    if any(token in lowered for token in ["ignite", "data grid", "cache cluster"]):
        return "ignite_status"
    if any(token in lowered for token in ["architecture", "design", "stack overview"]):
        return "architecture_info"
    if any(token in lowered for token in ["what changed", "changed today", "change log", "changes today"]):
        return "change_info"
    if any(token in lowered for token in ["cpu", "memory", "latency", "throughput", "slow", "performance"]):
        return "performance_issue"
    if any(token in lowered for token in ["error", "exception", "warn", "trace", "log"]):
        return "log_inquiry"
    if any(token in lowered for token in ["healthy", "health", "up", "status", "system"]):
        return "overall_health"
    return "unknown"


def _kafka_alerts_and_playbook(
    brokers: float,
    consumer_lag: float,
    under_replicated: float,
) -> tuple[str, list[dict[str, str]], list[str]]:
    status = "healthy"
    alerts: list[dict[str, str]] = []
    playbook: list[str] = []

    if brokers < 1:
        status = "critical"
        alerts.append(
            {
                "name": "KafkaBrokersDown",
                "severity": "critical",
                "condition": "Synthetic Kafka broker count is below 1.",
                "rationale": "The cluster appears unavailable to producers and consumers.",
            }
        )
        playbook.extend(
            [
                "Check broker container or process health and restore at least one reachable broker.",
                "Verify listeners, advertised listeners, and network reachability before re-enabling traffic.",
                "Pause risky producer or consumer rollouts until broker availability recovers.",
            ]
        )

    if under_replicated > 0:
        if status != "critical":
            status = "degraded"
        alerts.append(
            {
                "name": "KafkaUnderReplicatedPartitions",
                "severity": "warning" if under_replicated < 3 else "critical",
                "condition": f"Synthetic under-replicated partitions = {int(under_replicated)}.",
                "rationale": "Replication safety is reduced and data loss risk increases if another broker fails.",
            }
        )
        playbook.extend(
            [
                "Inspect broker health and ISR shrink events for the affected partitions.",
                "Confirm disk, CPU, and network are not throttling replication traffic.",
                "Delay partition reassignments or broker maintenance until replication catches up.",
            ]
        )

    if consumer_lag >= 1000:
        if status != "critical":
            status = "degraded"
        alerts.append(
            {
                "name": "KafkaConsumerLagHigh",
                "severity": "critical",
                "condition": f"Synthetic consumer lag = {int(consumer_lag)}.",
                "rationale": "Consumers are materially behind and end-to-end latency is likely user visible.",
            }
        )
        playbook.extend(
            [
                "Scale or restart lagging consumers after confirming they can commit offsets safely.",
                "Check whether downstream processing latency or broker fetch throttling is causing backlog growth.",
                "Review recent deployments or schema changes that may have slowed consumer processing.",
            ]
        )
    elif consumer_lag >= 250:
        if status == "healthy":
            status = "elevated"
        alerts.append(
            {
                "name": "KafkaConsumerLagElevated",
                "severity": "warning",
                "condition": f"Synthetic consumer lag = {int(consumer_lag)}.",
                "rationale": "Backlog is building and should be watched before it becomes user-impacting.",
            }
        )
        playbook.append("Track lag trend for the next few scrape intervals and pre-scale consumers if backlog keeps rising.")

    if not alerts:
        playbook.append("No Kafka remediation is required right now; keep monitoring broker count, lag, and replication health.")

    return status, alerts, _dedupe_steps(playbook)


def _ignite_alerts_and_playbook(
    nodes: float,
    cache_hit_rate: float,
    memory_pressure: float,
    rebalance: float,
) -> tuple[str, list[dict[str, str]], list[str]]:
    status = "healthy"
    alerts: list[dict[str, str]] = []
    playbook: list[str] = []

    if nodes < 1:
        status = "critical"
        alerts.append(
            {
                "name": "IgniteClusterUnavailable",
                "severity": "critical",
                "condition": "Synthetic Ignite node count is below 1.",
                "rationale": "The cluster appears unavailable and cache-backed workloads may fail open or fail closed.",
            }
        )
        playbook.extend(
            [
                "Recover at least one Ignite node and validate cluster discovery before restoring traffic.",
                "Check persistent storage, baseline topology, and recent restarts for cluster formation issues.",
                "Temporarily reduce cache-dependent load if the application has a safe degraded mode.",
            ]
        )

    if memory_pressure >= 85:
        if status != "critical":
            status = "degraded"
        alerts.append(
            {
                "name": "IgniteMemoryPressureHigh",
                "severity": "critical",
                "condition": f"Synthetic Ignite memory pressure = {round(memory_pressure, 1)}%.",
                "rationale": "The data grid is close to eviction, GC churn, or node instability.",
            }
        )
        playbook.extend(
            [
                "Reduce hot cache footprint or add capacity before the cluster reaches an eviction spiral.",
                "Inspect large keys, near-cache growth, and recent workload spikes that changed memory shape.",
                "Validate JVM and off-heap memory sizing assumptions against current traffic.",
            ]
        )
    elif memory_pressure >= 70:
        if status == "healthy":
            status = "elevated"
        alerts.append(
            {
                "name": "IgniteMemoryPressureElevated",
                "severity": "warning",
                "condition": f"Synthetic Ignite memory pressure = {round(memory_pressure, 1)}%.",
                "rationale": "The cluster is stable now but trending toward an unsafe memory envelope.",
            }
        )
        playbook.append("Watch the memory trend and defer heavy rebalance or batch jobs until pressure normalizes.")

    if cache_hit_rate < 0.90:
        if status != "critical":
            status = "degraded"
        alerts.append(
            {
                "name": "IgniteCacheHitRateLow",
                "severity": "warning",
                "condition": f"Synthetic cache hit rate = {round(cache_hit_rate * 100, 1)}%.",
                "rationale": "Cache misses are high enough to push more load onto backing services.",
            }
        )
        playbook.extend(
            [
                "Check whether hot keys expired unexpectedly or a recent deployment changed cache access patterns.",
                "Warm critical keys or extend TTLs for frequently reused data if the miss pattern is legitimate.",
            ]
        )

    if rebalance > 0:
        if status == "healthy":
            status = "elevated"
        alerts.append(
            {
                "name": "IgniteRebalanceInProgress",
                "severity": "info",
                "condition": "Synthetic Ignite rebalance flag is active.",
                "rationale": "Rebalance can temporarily amplify latency or memory pressure during topology change.",
            }
        )
        playbook.append("Avoid concurrent topology changes while rebalance is active and watch latency during partition movement.")

    if not alerts:
        playbook.append("No Ignite remediation is required right now; continue monitoring node count, cache hit rate, and memory pressure.")

    return status, alerts, _dedupe_steps(playbook)


def _dedupe_steps(steps: list[str]) -> list[str]:
    unique_steps: list[str] = []
    for step in steps:
        if step not in unique_steps:
            unique_steps.append(step)
    return unique_steps[:5]


def query_kafka_status() -> dict[str, Any]:
    brokers = _prometheus_scalar("aie_domain_kafka_brokers")
    consumer_lag = _prometheus_scalar("aie_domain_kafka_consumer_lag")
    under_replicated = _prometheus_scalar("aie_domain_kafka_under_replicated_partitions")

    service_metrics = _service_domain_metrics()
    brokers = service_metrics.get("aie_domain_kafka_brokers", brokers)
    consumer_lag = service_metrics.get("aie_domain_kafka_consumer_lag", consumer_lag)
    under_replicated = service_metrics.get("aie_domain_kafka_under_replicated_partitions", under_replicated)

    if brokers is None:
        return {
            "component": "kafka",
            "integration_status": "synthetic_signals_expected",
            "runtime_status": "signal_unavailable",
            "summary": "Kafka synthetic metrics are not visible in Prometheus yet. The sample service may need another scrape interval after startup.",
            "details": {},
        }

    status, alerts, playbook = _kafka_alerts_and_playbook(
        brokers=brokers,
        consumer_lag=consumer_lag or 0,
        under_replicated=under_replicated or 0,
    )

    findings: list[str] = []
    if brokers < 1:
        findings.append("no synthetic Kafka brokers are currently reported")
    if under_replicated and under_replicated > 0:
        findings.append(f"{int(under_replicated)} under-replicated partitions are reported")
    if consumer_lag is not None and consumer_lag >= 1000:
        findings.append(f"consumer lag is high at {int(consumer_lag)}")
    elif consumer_lag is not None and consumer_lag >= 250:
        findings.append(f"consumer lag is elevated at {int(consumer_lag)}")

    return {
        "component": "kafka",
        "integration_status": "synthetic_metrics",
        "runtime_status": "simulated_via_prometheus",
        "status": status,
        "summary": (
            "Kafka synthetic metrics look healthy."
            if not findings
            else "Kafka synthetic metrics show attention points: " + "; ".join(findings)
        ),
        "details": {
            "brokers": int(brokers),
            "consumer_lag": int(consumer_lag or 0),
            "under_replicated_partitions": int(under_replicated or 0),
        },
        "alert_recommendations": alerts,
        "remediation_playbook": playbook,
    }


def query_ignite_status() -> dict[str, Any]:
    nodes = _prometheus_scalar("aie_domain_ignite_nodes")
    cache_hit_rate = _prometheus_scalar("aie_domain_ignite_cache_hit_rate")
    memory_pressure = _prometheus_scalar("aie_domain_ignite_memory_pressure")
    rebalance = _prometheus_scalar("aie_domain_ignite_rebalance_in_progress")

    service_metrics = _service_domain_metrics()
    nodes = service_metrics.get("aie_domain_ignite_nodes", nodes)
    cache_hit_rate = service_metrics.get("aie_domain_ignite_cache_hit_rate", cache_hit_rate)
    memory_pressure = service_metrics.get("aie_domain_ignite_memory_pressure", memory_pressure)
    rebalance = service_metrics.get("aie_domain_ignite_rebalance_in_progress", rebalance)

    if nodes is None:
        return {
            "component": "ignite",
            "integration_status": "synthetic_signals_expected",
            "runtime_status": "signal_unavailable",
            "summary": "Ignite synthetic metrics are not visible in Prometheus yet. The sample service may need another scrape interval after startup.",
            "details": {},
        }

    status, alerts, playbook = _ignite_alerts_and_playbook(
        nodes=nodes,
        cache_hit_rate=cache_hit_rate or 0.0,
        memory_pressure=memory_pressure or 0.0,
        rebalance=rebalance or 0.0,
    )

    findings: list[str] = []
    if nodes < 1:
        findings.append("no synthetic Ignite nodes are currently reported")
    if memory_pressure is not None and memory_pressure >= 85:
        findings.append(f"memory pressure is high at {round(memory_pressure, 1)}%")
    elif memory_pressure is not None and memory_pressure >= 70:
        findings.append(f"memory pressure is elevated at {round(memory_pressure, 1)}%")
    if cache_hit_rate is not None and cache_hit_rate < 0.90:
        findings.append(f"cache hit rate is low at {round(cache_hit_rate * 100, 1)}%")
    if rebalance and rebalance > 0:
        findings.append("rebalance is currently in progress")

    return {
        "component": "ignite",
        "integration_status": "synthetic_metrics",
        "runtime_status": "simulated_via_prometheus",
        "status": status,
        "summary": (
            "Ignite synthetic metrics look healthy."
            if not findings
            else "Ignite synthetic metrics show attention points: " + "; ".join(findings)
        ),
        "details": {
            "cluster_nodes": int(nodes),
            "cache_hit_rate": round(cache_hit_rate or 0.0, 3),
            "memory_pressure": round(memory_pressure or 0.0, 1),
            "rebalance_in_progress": bool(rebalance and rebalance > 0),
        },
        "alert_recommendations": alerts,
        "remediation_playbook": playbook,
    }


def query_architecture_knowledge(question: str) -> dict[str, Any]:
    return query_knowledge(question)


def _extract_service_name(question: str) -> str:
    lowered = question.lower()
    if "sample-service" in lowered or "sample service" in lowered or "service a" in lowered:
        return "sample-service"
    return "sample-service"


def _summarize_up_result(result: list[dict[str, Any]]) -> dict[str, Any]:
    services: list[dict[str, Any]] = []
    down_targets: list[str] = []
    for item in result:
        metric = item.get("metric", {})
        target = metric.get("job") or metric.get("instance") or "unknown"
        value = item.get("value", [None, "0"])[1]
        status = "up" if str(value) == "1" else "down"
        services.append({"target": target, "status": status, "instance": metric.get("instance")})
        if status == "down":
            down_targets.append(target)
    return {
        "healthy": not down_targets,
        "targets": services,
        "down_targets": down_targets,
        "summary": "All scraped targets are up." if not down_targets else f"Down targets: {', '.join(down_targets)}",
    }


def _summarize_scalar_result(result: list[dict[str, Any]], label: str, unit: str = "%") -> dict[str, Any]:
    value = None
    if result:
        raw_value = result[0].get("value", [None, None])[1]
        if raw_value is not None:
            value = round(float(raw_value), 2)
    status = "unknown"
    if value is not None:
        if value >= 85:
            status = "high"
        elif value >= 70:
            status = "elevated"
        else:
            status = "normal"
    return {
        "metric": label,
        "value": value,
        "unit": unit,
        "status": status,
        "summary": f"{label} is {value}{unit} and considered {status}." if value is not None else f"{label} is unavailable.",
    }


def _summarize_custom_scalar(result: list[dict[str, Any]], label: str, unit: str, elevated_threshold: float | None = None, high_threshold: float | None = None, multiplier: float = 1.0) -> dict[str, Any]:
    value = None
    if result:
        raw_value = result[0].get("value", [None, None])[1]
        if raw_value is not None:
            value = round(float(raw_value) * multiplier, 2)

    status = "unknown"
    if value is not None:
        status = "normal"
        if high_threshold is not None and value >= high_threshold:
            status = "high"
        elif elevated_threshold is not None and value >= elevated_threshold:
            status = "elevated"

    return {
        "metric": label,
        "value": value,
        "unit": unit,
        "status": status,
        "summary": f"{label} is {value}{unit} and considered {status}." if value is not None else f"{label} is unavailable.",
    }


def query_metrics(question: str, query_type: str) -> dict[str, Any]:
    if query_type == "performance_issue":
        lowered = question.lower()
        if "memory" in lowered:
            promql = '100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))'
            label = "memory_usage"
        else:
            promql = '100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))'
            label = "cpu_usage"
        payload = _prometheus_query(promql)
        result = payload.get("data", {}).get("result", [])
        return {
            "query": promql,
            "kind": label,
            "raw_status": payload.get("status", "unknown"),
            "summary": _summarize_scalar_result(result, label),
        }

    promql = "up"
    payload = _prometheus_query(promql)
    result = payload.get("data", {}).get("result", [])
    return {
        "query": promql,
        "kind": "overall_health",
        "raw_status": payload.get("status", "unknown"),
        "summary": _summarize_up_result(result),
    }


def _extract_log_lines(streams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for stream in streams[:5]:
        labels = stream.get("stream", {})
        for timestamp, line in stream.get("values", [])[:5]:
            lines.append({"timestamp": timestamp, "labels": labels, "line": line.strip()})
    return lines


def _looks_like_actionable_error(line: str) -> bool:
    lowered = line.lower()
    if "errors=false" in lowered:
        return False
    return any(token in lowered for token in ["level=error", " exception", "traceback", "failed", " panic"])


def _looks_like_performance_signal(line: str) -> bool:
    lowered = line.lower()
    if "errors=false" in lowered:
        return False
    if "caller=metrics.go" in lowered or "caller=engine.go" in lowered or "query=" in lowered:
        return False
    return any(token in lowered for token in ["slow request", "timeout", "failed", "level=error", "terminated", "deadlineexceeded"])


def _parse_service_timer_metrics(metrics_text: str) -> dict[str, float]:
    count_pattern = re.compile(r'^http_server_requests_seconds_count\{[^}]*uri="(?P<uri>[^"]+)"[^}]*\}\s+(?P<value>\S+)$')
    sum_pattern = re.compile(r'^http_server_requests_seconds_sum\{[^}]*uri="(?P<uri>[^"]+)"[^}]*\}\s+(?P<value>\S+)$')

    counts: dict[str, float] = {}
    sums: dict[str, float] = {}

    for line in metrics_text.splitlines():
        count_match = count_pattern.match(line)
        if count_match:
            counts[count_match.group("uri")] = counts.get(count_match.group("uri"), 0.0) + float(count_match.group("value"))
            continue

        sum_match = sum_pattern.match(line)
        if sum_match:
            sums[sum_match.group("uri")] = sums.get(sum_match.group("uri"), 0.0) + float(sum_match.group("value"))

    return {
        "slow_count": counts.get("/slow", 0.0),
        "slow_sum": sums.get("/slow", 0.0),
        "ping_count": counts.get("/ping", 0.0),
        "ping_sum": sums.get("/ping", 0.0),
    }


def _parse_service_domain_metrics(metrics_text: str) -> dict[str, float]:
    wanted_metrics = {
        "aie_domain_kafka_brokers",
        "aie_domain_kafka_consumer_lag",
        "aie_domain_kafka_under_replicated_partitions",
        "aie_domain_ignite_nodes",
        "aie_domain_ignite_cache_hit_rate",
        "aie_domain_ignite_memory_pressure",
        "aie_domain_ignite_rebalance_in_progress",
    }
    parsed: dict[str, float] = {}
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 2:
            continue
        metric_name, raw_value = parts
        if metric_name in wanted_metrics:
            parsed[metric_name] = float(raw_value)
    return parsed


def _service_domain_metrics() -> dict[str, float]:
    try:
        return _parse_service_domain_metrics(_service_metrics_text())
    except Exception:
        return {}


def _apply_sample_service_metric_fallback(metric_summaries: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = _parse_service_timer_metrics(_service_metrics_text())
    except Exception:
        return metric_summaries

    total_count = parsed["slow_count"] + parsed["ping_count"]
    total_sum = parsed["slow_sum"] + parsed["ping_sum"]

    if total_count > 0:
        metric_summaries["service_latency_ms"]["summary"] = _summarize_custom_scalar(
            result=[{"value": [None, str(total_sum / total_count)]}],
            label="service_latency_ms",
            unit="ms",
            elevated_threshold=300.0,
            high_threshold=700.0,
            multiplier=1000.0,
        )
        metric_summaries["request_count"]["summary"] = _summarize_custom_scalar(
            result=[{"value": [None, str(total_count)]}],
            label="request_count",
            unit="requests",
            multiplier=1.0,
        )

    if parsed["slow_count"] > 0:
        metric_summaries["slow_endpoint_latency_ms"]["summary"] = _summarize_custom_scalar(
            result=[{"value": [None, str(parsed["slow_sum"] / parsed["slow_count"])]}],
            label="slow_endpoint_latency_ms",
            unit="ms",
            elevated_threshold=500.0,
            high_threshold=900.0,
            multiplier=1000.0,
        )

    return metric_summaries


def query_logs(question: str, query_type: str) -> dict[str, Any]:
    lowered = question.lower()
    error_focused = query_type == "log_inquiry" and any(token in lowered for token in ["error", "exception"])
    service_name = _extract_service_name(question)
    if error_focused:
        logql = '{job="docker"} |~ "(?i)error|exception"'
    elif query_type == "performance_issue":
        if service_name == "sample-service":
            logql = '{job="docker"} |= "sample-service slow request simulated"'
        else:
            logql = '{job="docker"} |~ "(?i)slow|timeout|error|exception|failed"'
    elif "ping" in lowered or "sample" in lowered or "service" in lowered:
        logql = '{job="docker"} |= "Ping request received"'
    else:
        logql = '{job="docker"}'

    payload = _loki_query(logql)
    streams = payload.get("data", {}).get("result", [])
    lines = _extract_log_lines(streams)
    if error_focused:
        lines = [item for item in lines if _looks_like_actionable_error(item["line"])]
    elif query_type == "performance_issue":
        lines = [item for item in lines if _looks_like_performance_signal(item["line"])]
    unique_examples: list[str] = []
    for item in lines:
        line = item["line"]
        if line not in unique_examples:
            unique_examples.append(line)
        if len(unique_examples) == 3:
            break

    return {
        "query": logql,
        "line_count": len(lines),
        "examples": unique_examples,
        "summary": (
            f"Found {len(lines)} matching log lines in the last 10 minutes."
            if lines
            else "No matching log lines found in the last 10 minutes."
        ),
    }


def query_performance_insights(question: str) -> dict[str, Any]:
    service_name = _extract_service_name(question)

    queries = {
        "service_latency_ms": (
            f'sum(http_server_requests_seconds_sum{{job="{service_name}",uri=~"/ping|/slow"}}) '
            f'/ sum(http_server_requests_seconds_count{{job="{service_name}",uri=~"/ping|/slow"}})',
            "ms",
            300.0,
            700.0,
            1000.0,
        ),
        "slow_endpoint_latency_ms": (
            f'sum(http_server_requests_seconds_sum{{job="{service_name}",uri="/slow"}}) '
            f'/ sum(http_server_requests_seconds_count{{job="{service_name}",uri="/slow"}})',
            "ms",
            500.0,
            900.0,
            1000.0,
        ),
        "request_count": (
            f'sum(http_server_requests_seconds_count{{job="{service_name}",uri=~"/ping|/slow"}})',
            "requests",
            None,
            None,
            1.0,
        ),
        "error_count": (
            f'sum(http_server_requests_seconds_count{{job="{service_name}",status=~"5.."}})',
            "errors",
            1.0,
            5.0,
            1.0,
        ),
        "cpu_usage": (
            '100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))',
            "%",
            70.0,
            85.0,
            1.0,
        ),
        "memory_usage": (
            '100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))',
            "%",
            70.0,
            85.0,
            1.0,
        ),
    }

    metric_summaries: dict[str, Any] = {}
    for label, (promql, unit, elevated_threshold, high_threshold, multiplier) in queries.items():
        payload = _prometheus_query(promql)
        result = payload.get("data", {}).get("result", [])
        metric_summaries[label] = {
            "query": promql,
            "summary": _summarize_custom_scalar(
                result=result,
                label=label,
                unit=unit,
                elevated_threshold=elevated_threshold,
                high_threshold=high_threshold,
                multiplier=multiplier,
            ),
        }

    if service_name == "sample-service":
        metric_summaries = _apply_sample_service_metric_fallback(metric_summaries)

    logs_data = query_logs(f"{service_name} slow timeout error", "performance_issue")

    findings: list[str] = []
    likely_causes: list[str] = []

    slow_endpoint = metric_summaries["slow_endpoint_latency_ms"]["summary"]
    service_latency = metric_summaries["service_latency_ms"]["summary"]
    cpu_usage = metric_summaries["cpu_usage"]["summary"]
    memory_usage = metric_summaries["memory_usage"]["summary"]

    if slow_endpoint.get("status") in {"elevated", "high"}:
        findings.append(slow_endpoint["summary"])
        likely_causes.append("application request latency is elevated on the /slow path")
    elif service_latency.get("status") in {"elevated", "high"}:
        findings.append(service_latency["summary"])

    if cpu_usage.get("status") in {"elevated", "high"}:
        findings.append(cpu_usage["summary"])
        likely_causes.append("host CPU pressure may be contributing")

    if memory_usage.get("status") in {"elevated", "high"}:
        findings.append(memory_usage["summary"])
        likely_causes.append("host memory pressure may be contributing")

    if logs_data["line_count"]:
        findings.append(logs_data["summary"])
        likely_causes.append("recent logs contain slow, timeout, or failed-operation signals")

    if not findings:
        findings.append("No obvious latency or infrastructure anomaly is visible in the current 5 minute window.")

    if not likely_causes:
        likely_causes.append("there is not enough evidence yet to isolate a root cause")

    return {
        "service": service_name,
        "metrics": metric_summaries,
        "logs": logs_data,
        "findings": findings,
        "likely_causes": likely_causes,
        "summary": "; ".join(findings[:3]),
    }
