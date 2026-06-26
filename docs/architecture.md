# AIEverywhere Architecture

AIEverywhere is a local observability demo stack built as a monorepo.

## Core Components

- Grafana OSS provides dashboards and log exploration.
- Prometheus scrapes metrics from Prometheus itself, nodeexporter, and the sample Spring Boot service.
- Loki stores logs and is fed by Promtail.
- Ollama provides local offline LLM inference for the agent.
- The agent is a FastAPI and LangGraph service that answers observability questions.
- The sample Spring Boot service exposes Actuator and Prometheus metrics.

## Current Runtime Model

- Primary model: qwen2.5-coder:14b
- Fallback model: deepseek-coder:6.7b

## Messaging And Data Grid Plan

- Kafka is modeled in Day 5 through synthetic metrics exported by the sample Spring Boot service.
- Apache Ignite is modeled in Day 5 through synthetic metrics exported by the sample Spring Boot service.
- In the current repo state, Kafka and Ignite are not yet running containers.
- The agent reads those simulated component signals from Prometheus so domain questions are grounded in live telemetry.

## Agent Reasoning Model

- Day 2: single-turn health and log inspection.
- Day 3: conversation memory and basic root-cause heuristics.
- Day 4 target: domain-specific knowledge and component-aware questions.
- Day 5 target: synthetic Kafka and Ignite signal ingestion via Prometheus-backed domain metrics.
- Day 6 target: alert recommendations and remediation playbooks derived from domain signal thresholds.
