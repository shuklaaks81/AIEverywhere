# Demo Change Log

## Day 1

- Created the monorepo scaffold.
- Brought up Grafana, Prometheus, Loki, Promtail, Ollama, nodeexporter, and the sample service.

## Day 2

- Implemented the first working LangGraph agent flow.
- Added Prometheus and Loki read-only tools.
- Added Ollama synthesis with fallback behavior.

## Day 3

- Added conversation memory with conversation_id support.
- Added a root-cause branch for performance questions.
- Added a slow endpoint in the sample service for repeatable performance demos.

## Day 4 Status

- Architecture and domain knowledge retrieval is being added.
- Kafka and Ignite are currently planned components, not yet active containers.

## Day 5 Status

- Added synthetic Kafka and Ignite metrics to the sample service for Prometheus ingestion.
- Switched Kafka and Ignite agent answers from static placeholders to Prometheus-backed synthetic telemetry.

## Day 6 Status

- Added alert recommendations for Kafka and Ignite based on synthetic domain thresholds.
- Added remediation playbook steps so domain answers now include actionable next steps.

## Day 7 Status

- Polished operator UX with concise structured answers built from Summary, section blocks, alerts, and next steps.
- Added top-level structured response fields so UI clients can render operator guidance without parsing freeform text.
- Added a lightweight operator console that renders structured response fields directly.
- Added snapshot-style tests for representative operator response payloads.
- Added a raw JSON inspection mode in the operator console.
- Expanded snapshot coverage to health and performance payloads.
