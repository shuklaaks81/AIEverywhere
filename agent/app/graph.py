from __future__ import annotations

from typing import Any

import httpx
from langgraph.graph import END, START, StateGraph

from app.config import settings
from app.prompts import ANSWER_SYSTEM_PROMPT, ANSWER_WITH_MEMORY_PROMPT
from app.schemas import ObservabilityState
from app.tools import (
    classify_query_type,
    query_architecture_knowledge,
    query_ignite_status,
    query_kafka_status,
    query_logs,
    query_metrics,
    query_performance_insights,
)


def _format_context_block(context: dict[str, Any]) -> str:
    if not context:
        return "No data collected."
    return str(context)


def _resolve_query_from_history(question: str, chat_history: str) -> str:
    lowered = question.lower().strip()
    if not chat_history:
        return question

    follow_up_markers = ["it", "that", "then", "earlier", "now", "still", "fixed", "same issue", "why"]
    if any(lowered.startswith(marker) for marker in follow_up_markers) or any(f" {marker} " in lowered for marker in follow_up_markers):
        return f"Conversation context:\n{chat_history}\n\nFollow-up question: {question}"
    return question


def _call_ollama(
    question: str,
    resolved_question: str,
    chat_history: str,
    query_type: str,
    metrics_data: dict[str, Any],
    logs_data: dict[str, Any],
    root_cause_data: dict[str, Any],
    knowledge_data: dict[str, Any],
    domain_data: dict[str, Any],
    requested_model: str | None,
) -> tuple[str, str]:
    prompt = ANSWER_SYSTEM_PROMPT + "\n\n" + ANSWER_WITH_MEMORY_PROMPT.format(
        chat_history=chat_history or "No previous conversation.",
        resolved_question=resolved_question,
        question=question,
        query_type=query_type,
        metrics_context=_format_context_block(metrics_data),
        logs_context=_format_context_block(logs_data),
        root_cause_context=_format_context_block(root_cause_data),
        knowledge_context=_format_context_block(knowledge_data),
        domain_context=_format_context_block(domain_data),
    )
    models_to_try = [requested_model or settings.ollama_model]
    fallback_model = settings.ollama_fallback_model
    if fallback_model and fallback_model not in models_to_try:
        models_to_try.append(fallback_model)

    last_error = "Unknown Ollama failure"
    for model in models_to_try:
        payload = {"model": model, "prompt": prompt, "stream": False, "options": {"temperature": 0.2}}
        try:
            with httpx.Client(timeout=settings.ollama_timeout_seconds) as client:
                response = client.post(f"{settings.ollama_url}/api/generate", json=payload)
                response.raise_for_status()
                body = response.json()
            answer = body.get("response", "").strip()
            if answer:
                return answer, model
            last_error = f"Model {model} returned an empty response"
        except Exception as exc:
            last_error = str(exc)
    raise RuntimeError(last_error)


def _fallback_answer(state: ObservabilityState, error: str) -> str:
    return (
        "LLM synthesis is unavailable, so this is a direct observability summary. "
        f"Query type: {state.get('query_type', 'unknown')}. "
        f"Metrics: {state.get('metrics_data', {})}. "
        f"Logs: {state.get('logs_data', {})}. "
        f"Root cause: {state.get('root_cause_data', {})}. "
        f"Knowledge: {state.get('knowledge_data', {})}. "
        f"Domain: {state.get('domain_data', {})}. "
        f"LLM error: {error}."
    )


def _section(title: str, items: list[str]) -> dict[str, Any]:
    return {"title": title, "items": [item for item in items if item]}


def _extract_doc_bullets(content: str, limit: int = 5) -> list[str]:
    bullets: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullets.append(stripped[2:])
        if len(bullets) == limit:
            break
    return bullets


def _parse_markdown_sections(content: str) -> list[tuple[str, list[str]]]:
    sections: list[tuple[str, list[str]]] = []
    current_title: str | None = None
    current_items: list[str] = []

    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            if current_title is not None:
                sections.append((current_title, current_items))
            current_title = stripped[3:]
            current_items = []
            continue
        if stripped.startswith("- ") and current_title is not None:
            current_items.append(stripped[2:])

    if current_title is not None:
        sections.append((current_title, current_items))

    return sections


def _render_operator_answer(summary: str, sections: list[dict[str, Any]]) -> str:
    rendered_sections: list[str] = [f"Summary: {summary}"]
    for section in sections:
        items = section.get("items", [])
        if not items:
            continue
        rendered_sections.append(section.get("title", "Details") + ":\n" + "\n".join(f"- {item}" for item in items))
    return "\n\n".join(rendered_sections)


def _build_operator_view(state: ObservabilityState, summary_text: str) -> dict[str, Any]:
    query_type = state.get("query_type", "unknown")
    status: str | None = None
    sections: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []
    next_steps: list[str] = []
    summary = summary_text.strip()

    if query_type in {"kafka_status", "ignite_status"}:
        domain_data = state.get("domain_data", {})
        details = domain_data.get("details", {})
        status = domain_data.get("status") or domain_data.get("runtime_status")
        summary = domain_data.get("summary", summary)
        alerts = domain_data.get("alert_recommendations", [])
        next_steps = domain_data.get("remediation_playbook", [])[:3]
        if query_type == "kafka_status":
            sections = [
                _section(
                    "Signals",
                    [
                        f"Status: {status or 'unknown'}",
                        f"Brokers: {details.get('brokers', 'unknown')}",
                        f"Consumer lag: {details.get('consumer_lag', 'unknown')}",
                        f"Under-replicated partitions: {details.get('under_replicated_partitions', 'unknown')}",
                    ],
                ),
                _section(
                    "Alerts",
                    [f"{alert.get('name', 'unknown')} ({alert.get('severity', 'unknown')}): {alert.get('condition', '')}" for alert in alerts[:3]],
                ),
                _section("Next Steps", next_steps),
            ]
        else:
            sections = [
                _section(
                    "Signals",
                    [
                        f"Status: {status or 'unknown'}",
                        f"Cluster nodes: {details.get('cluster_nodes', 'unknown')}",
                        f"Cache hit rate: {details.get('cache_hit_rate', 'unknown')}",
                        f"Memory pressure: {details.get('memory_pressure', 'unknown')}%",
                        f"Rebalance in progress: {details.get('rebalance_in_progress', 'unknown')}",
                    ],
                ),
                _section(
                    "Alerts",
                    [f"{alert.get('name', 'unknown')} ({alert.get('severity', 'unknown')}): {alert.get('condition', '')}" for alert in alerts[:3]],
                ),
                _section("Next Steps", next_steps),
            ]

    elif query_type == "overall_health":
        health_summary = state.get("metrics_data", {}).get("summary", {})
        healthy = health_summary.get("healthy")
        status = "healthy" if healthy else "degraded"
        summary = health_summary.get("summary", summary)
        down_targets = health_summary.get("down_targets", [])
        next_steps = [f"Inspect the Prometheus scrape target for {target}." for target in down_targets[:3]]
        sections = [
            _section(
                "Targets",
                [f"{target.get('target', 'unknown')}: {target.get('status', 'unknown')}" for target in health_summary.get("targets", [])[:6]],
            ),
            _section("Next Steps", next_steps),
        ]

    elif query_type == "performance_issue":
        root_cause_data = state.get("root_cause_data", {})
        findings = root_cause_data.get("findings", [])
        likely_causes = root_cause_data.get("likely_causes", [])
        status = "degraded" if findings and "No obvious latency" not in findings[0] else "healthy"
        summary = root_cause_data.get("summary", summary)
        next_steps = [f"Check whether {cause}." for cause in likely_causes[:3]]
        sections = [
            _section("Findings", findings[:4]),
            _section("Likely Causes", likely_causes[:3]),
            _section("Next Steps", next_steps),
        ]

    elif query_type == "log_inquiry":
        logs_data = state.get("logs_data", {})
        status = "attention" if logs_data.get("line_count", 0) else "clear"
        summary = logs_data.get("summary", summary)
        next_steps = ["Inspect the matching service logs in Grafana or Loki for full context."] if logs_data.get("line_count", 0) else []
        sections = [
            _section("Examples", logs_data.get("examples", [])[:3]),
            _section("Next Steps", next_steps),
        ]

    elif query_type == "architecture_info":
        knowledge_data = state.get("knowledge_data", {})
        content = knowledge_data.get("content", "")
        parsed_sections = dict(_parse_markdown_sections(content))
        status = "reference"
        summary = "Local architecture reference retrieved with runtime model and planned component context."
        sections = [
            _section("Core Components", parsed_sections.get("Core Components", _extract_doc_bullets(content, limit=6))[:6]),
            _section("Runtime Model", parsed_sections.get("Current Runtime Model", [])[:4]),
            _section("Planned Components", parsed_sections.get("Messaging And Data Grid Plan", [])[:5]),
            _section("Reasoning Model", parsed_sections.get("Agent Reasoning Model", [])[:5]),
            _section("Source", [knowledge_data.get("source", "architecture.md")]),
        ]

    elif query_type == "change_info":
        knowledge_data = state.get("knowledge_data", {})
        content = knowledge_data.get("content", "")
        parsed_sections = _parse_markdown_sections(content)
        status = "reference"
        summary = "Recent recorded project changes retrieved from the local change log."
        sections = [
            *[_section(title, items[:4]) for title, items in parsed_sections[:4]],
            _section("Source", [knowledge_data.get("source", "change-log.md")]),
        ]

    else:
        status = state.get("query_type", "unknown")
        sections = [_section("Details", [summary])]

    return {
        "operator_status": status,
        "operator_summary": summary,
        "answer_sections": sections,
        "recommended_alerts": alerts,
        "next_steps": next_steps,
    }


def _apply_day2_guardrails(state: ObservabilityState, answer: str) -> str:
    query_type = state.get("query_type")
    lowered = answer.lower()

    if query_type == "architecture_info":
        knowledge_data = state.get("knowledge_data", {})
        content = knowledge_data.get("content")
        if content:
            return (
                "The current demo architecture is documented locally and consists of Docker Compose services for "
                "Grafana, Prometheus, Loki, Promtail, Node Exporter, Ollama, the FastAPI agent, and the sample Spring Boot service. "
                f"Source: {knowledge_data.get('source', 'architecture.md')}."
            )

    if query_type == "change_info":
        knowledge_data = state.get("knowledge_data", {})
        content = knowledge_data.get("content")
        if content:
            first_lines = " ".join(line.strip("- ") for line in content.splitlines() if line.strip()[:1] in {"#", "-"})
            return f"The local change log is available in {knowledge_data.get('source', 'change-log.md')}. Key recorded updates: {first_lines[:280]}"

    if query_type == "kafka_status":
        domain_data = state.get("domain_data", {})
        if domain_data.get("runtime_status") == "not_deployed":
            return (
                "Kafka is not deployed in the current demo stack. It is marked as a planned integration, "
                "so there are no live broker, partition, or consumer lag metrics yet."
            )
        if domain_data.get("runtime_status") == "simulated_via_prometheus":
            details = domain_data.get("details", {})
            alerts = domain_data.get("alert_recommendations", [])
            playbook = domain_data.get("remediation_playbook", [])
            alert_text = " ".join(
                f"Recommended alert {alert.get('name', 'unknown')} with severity {alert.get('severity', 'unknown')}."
                for alert in alerts[:2]
            )
            playbook_text = " ".join(
                f"Next step {index + 1}: {step}"
                for index, step in enumerate(playbook[:3])
            )
            return (
                f"Kafka synthetic telemetry status is {domain_data.get('status', 'unknown')}. "
                f"Brokers: {details.get('brokers', 'unknown')}, consumer lag: {details.get('consumer_lag', 'unknown')}, "
                f"under-replicated partitions: {details.get('under_replicated_partitions', 'unknown')}. "
                f"{domain_data.get('summary', '')} {alert_text} {playbook_text}"
            ).strip()

    if query_type == "ignite_status":
        domain_data = state.get("domain_data", {})
        if domain_data.get("runtime_status") == "not_deployed":
            return (
                "Ignite is not deployed in the current demo stack. It is marked as a planned integration, "
                "so there are no live cluster or cache health signals yet."
            )
        if domain_data.get("runtime_status") == "simulated_via_prometheus":
            details = domain_data.get("details", {})
            alerts = domain_data.get("alert_recommendations", [])
            playbook = domain_data.get("remediation_playbook", [])
            alert_text = " ".join(
                f"Recommended alert {alert.get('name', 'unknown')} with severity {alert.get('severity', 'unknown')}."
                for alert in alerts[:2]
            )
            playbook_text = " ".join(
                f"Next step {index + 1}: {step}"
                for index, step in enumerate(playbook[:3])
            )
            return (
                f"Ignite synthetic telemetry status is {domain_data.get('status', 'unknown')}. "
                f"Nodes: {details.get('cluster_nodes', 'unknown')}, cache hit rate: {details.get('cache_hit_rate', 'unknown')}, "
                f"memory pressure: {details.get('memory_pressure', 'unknown')}%, rebalance in progress: {details.get('rebalance_in_progress', 'unknown')}. "
                f"{domain_data.get('summary', '')} {alert_text} {playbook_text}"
            ).strip()

    if query_type == "overall_health":
        summary = state.get("metrics_data", {}).get("summary", {})
        if summary.get("healthy") is False and any(token in lowered for token in ["healthy", "all scraped targets are up"]):
            down_targets = ", ".join(summary.get("down_targets", [])) or "unknown targets"
            return f"The system is not fully healthy. Prometheus reports these down targets: {down_targets}."

    if query_type == "log_inquiry":
        logs_data = state.get("logs_data", {})
        line_count = logs_data.get("line_count", 0)
        examples = logs_data.get("examples", [])
        if line_count and any(token in lowered for token in ["no errors", "no error", "no exceptions"]):
            example_text = examples[0] if examples else "A matching error line was found."
            return (
                f"I found {line_count} log lines that look error-related in the last 10 minutes. "
                f"Example: {example_text}"
            )

    if query_type == "performance_issue":
        root_cause_data = state.get("root_cause_data", {})
        findings = root_cause_data.get("findings", [])
        if findings and any(token in lowered for token in ["no issue", "no problem", "looks normal"]):
            return f"I found performance signals worth checking: {'; '.join(findings[:2])}"

    return answer


def classify_node(state: ObservabilityState) -> ObservabilityState:
    resolved_query = _resolve_query_from_history(state["user_query"], state.get("chat_history", ""))
    return {
        "resolved_query": resolved_query,
        "query_type": classify_query_type(resolved_query),
        "used_sources": [],
    }


def should_fetch_metrics(state: ObservabilityState) -> str:
    if state["query_type"] in {"architecture_info", "change_info"}:
        return "knowledge"
    if state["query_type"] in {"kafka_status", "ignite_status"}:
        return "domain"
    if state["query_type"] == "performance_issue":
        return "root_cause"
    if state["query_type"] in {"overall_health", "unknown"}:
        return "fetch_metrics"
    return "fetch_logs"


def fetch_metrics_node(state: ObservabilityState) -> ObservabilityState:
    try:
        metrics_data = query_metrics(state["user_query"], state["query_type"])
        return {"metrics_data": metrics_data, "used_sources": state.get("used_sources", []) + ["prometheus"]}
    except Exception as exc:
        return {
            "metrics_data": {
                "error": str(exc),
                "summary": "Metrics lookup failed.",
            }
        }


def root_cause_node(state: ObservabilityState) -> ObservabilityState:
    try:
        root_cause_data = query_performance_insights(state["resolved_query"])
        return {
            "metrics_data": root_cause_data.get("metrics", {}),
            "logs_data": root_cause_data.get("logs", {}),
            "root_cause_data": root_cause_data,
            "used_sources": state.get("used_sources", []) + ["prometheus", "loki"],
        }
    except Exception as exc:
        return {
            "root_cause_data": {
                "error": str(exc),
                "summary": "Root cause lookup failed.",
            }
        }


def knowledge_node(state: ObservabilityState) -> ObservabilityState:
    try:
        knowledge_data = query_architecture_knowledge(state["resolved_query"])
        return {
            "knowledge_data": knowledge_data,
            "used_sources": state.get("used_sources", []) + [f"knowledge:{knowledge_data.get('source', 'docs')}"]
        }
    except Exception as exc:
        return {
            "knowledge_data": {
                "error": str(exc),
                "summary": "Knowledge lookup failed.",
            }
        }


def domain_node(state: ObservabilityState) -> ObservabilityState:
    try:
        if state["query_type"] == "kafka_status":
            domain_data = query_kafka_status()
        else:
            domain_data = query_ignite_status()
        return {
            "domain_data": domain_data,
            "used_sources": state.get("used_sources", []) + [domain_data["component"]],
        }
    except Exception as exc:
        return {
            "domain_data": {
                "error": str(exc),
                "summary": "Domain lookup failed.",
            }
        }


def should_fetch_logs(state: ObservabilityState) -> str:
    if state["query_type"] == "overall_health":
        healthy = state.get("metrics_data", {}).get("summary", {}).get("healthy")
        return "fetch_logs" if healthy is False else "generate_answer"
    if state["query_type"] in {"log_inquiry", "unknown"}:
        return "fetch_logs"
    return "generate_answer"


def fetch_logs_node(state: ObservabilityState) -> ObservabilityState:
    try:
        logs_data = query_logs(state["user_query"], state["query_type"])
        return {"logs_data": logs_data, "used_sources": state.get("used_sources", []) + ["loki"]}
    except Exception as exc:
        return {
            "logs_data": {
                "error": str(exc),
                "summary": "Log lookup failed.",
            }
        }


def generate_answer_node(state: ObservabilityState) -> ObservabilityState:
    try:
        answer, model_used = _call_ollama(
            question=state["user_query"],
            resolved_question=state.get("resolved_query", state["user_query"]),
            chat_history=state.get("chat_history", ""),
            query_type=state["query_type"],
            metrics_data=state.get("metrics_data", {}),
            logs_data=state.get("logs_data", {}),
            root_cause_data=state.get("root_cause_data", {}),
            knowledge_data=state.get("knowledge_data", {}),
            domain_data=state.get("domain_data", {}),
            requested_model=state.get("requested_model"),
        )
        answer = _apply_day2_guardrails(state, answer)
        operator_view = _build_operator_view(state, answer)
        return {
            "answer": _render_operator_answer(operator_view["operator_summary"], operator_view["answer_sections"]),
            "operator_status": operator_view["operator_status"],
            "operator_summary": operator_view["operator_summary"],
            "answer_sections": operator_view["answer_sections"],
            "recommended_alerts": operator_view["recommended_alerts"],
            "next_steps": operator_view["next_steps"],
            "used_sources": state.get("used_sources", []) + [f"ollama:{model_used}"],
        }
    except Exception as exc:
        answer = _fallback_answer(state, str(exc))
        operator_view = _build_operator_view(state, answer)
        return {
            "answer": _render_operator_answer(operator_view["operator_summary"], operator_view["answer_sections"]),
            "operator_status": operator_view["operator_status"],
            "operator_summary": operator_view["operator_summary"],
            "answer_sections": operator_view["answer_sections"],
            "recommended_alerts": operator_view["recommended_alerts"],
            "next_steps": operator_view["next_steps"],
        }


def build_graph():
    graph = StateGraph(ObservabilityState)
    graph.add_node("classify", classify_node)
    graph.add_node("fetch_metrics", fetch_metrics_node)
    graph.add_node("fetch_logs", fetch_logs_node)
    graph.add_node("root_cause", root_cause_node)
    graph.add_node("knowledge", knowledge_node)
    graph.add_node("domain", domain_node)
    graph.add_node("generate_answer", generate_answer_node)
    graph.add_edge(START, "classify")
    graph.add_conditional_edges(
        "classify",
        should_fetch_metrics,
        {
            "fetch_metrics": "fetch_metrics",
            "fetch_logs": "fetch_logs",
            "root_cause": "root_cause",
            "knowledge": "knowledge",
            "domain": "domain",
        },
    )
    graph.add_conditional_edges(
        "fetch_metrics",
        should_fetch_logs,
        {
            "fetch_logs": "fetch_logs",
            "generate_answer": "generate_answer",
        },
    )
    graph.add_edge("root_cause", "generate_answer")
    graph.add_edge("knowledge", "generate_answer")
    graph.add_edge("domain", "generate_answer")
    graph.add_edge("fetch_logs", "generate_answer")
    graph.add_edge("generate_answer", END)
    return graph.compile()


agent_graph = build_graph()
