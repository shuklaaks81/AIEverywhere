from typing import Any, Literal, TypedDict

from pydantic import BaseModel, Field


QueryType = Literal[
    "overall_health",
    "performance_issue",
    "log_inquiry",
    "architecture_info",
    "kafka_status",
    "ignite_status",
    "change_info",
    "unknown",
]


class QueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=2000)
    model: str | None = None
    conversation_id: str | None = None


class AlertRecommendation(BaseModel):
    name: str
    severity: str
    condition: str
    rationale: str


class ResponseSection(BaseModel):
    title: str
    items: list[str] = Field(default_factory=list)


class QueryResponse(BaseModel):
    answer: str
    status: str | None = None
    summary: str
    sections: list[ResponseSection] = Field(default_factory=list)
    alerts: list[AlertRecommendation] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)
    query_type: QueryType
    conversation_id: str
    used_sources: list[str]
    context: dict[str, Any]


class ObservabilityState(TypedDict, total=False):
    user_query: str
    resolved_query: str
    requested_model: str | None
    conversation_id: str
    chat_history: str
    query_type: QueryType
    metrics_data: dict[str, Any]
    logs_data: dict[str, Any]
    root_cause_data: dict[str, Any]
    knowledge_data: dict[str, Any]
    domain_data: dict[str, Any]
    operator_status: str
    operator_summary: str
    answer_sections: list[dict[str, Any]]
    recommended_alerts: list[dict[str, Any]]
    next_steps: list[str]
    answer: str
    used_sources: list[str]
