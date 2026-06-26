import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pathlib import Path

from app.config import settings
from app.demo import build_demo_response, list_demo_scenarios
from app.graph import agent_graph
from app.memory import get_or_create_conversation_id, load_chat_history, save_turn
from app.schemas import AlertRecommendation, ObservabilityState, QueryRequest, QueryResponse, ResponseSection

app = FastAPI(title="AIEverywhere Agent", version="0.4.0")

STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config")
def config() -> dict[str, str | float]:
    return settings.model_dump()


@app.get("/console")
def console() -> FileResponse:
    return FileResponse(STATIC_DIR / "operator-console.html")


@app.get("/query/demo", response_model=QueryResponse)
def demo_query(scenario: str = Query(default="kafka")) -> QueryResponse:
    try:
        return build_demo_response(scenario)
    except KeyError as exc:
        available = ", ".join(list_demo_scenarios())
        raise HTTPException(status_code=404, detail=f"Unknown demo scenario '{scenario}'. Available scenarios: {available}") from exc


@app.post("/query", response_model=QueryResponse)
@app.post("/ask", response_model=QueryResponse)
def query(request: QueryRequest) -> QueryResponse:
    conversation_id = get_or_create_conversation_id(request.conversation_id)
    initial_state: ObservabilityState = {
        "user_query": request.question,
        "requested_model": request.model,
        "conversation_id": conversation_id,
        "chat_history": load_chat_history(conversation_id),
    }
    try:
        result = agent_graph.invoke(initial_state)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream query failed: {exc}") from exc

    answer = result.get("answer", "No answer generated.")
    save_turn(conversation_id, request.question, answer)

    return QueryResponse(
        answer=answer,
        status=result.get("operator_status"),
        summary=result.get("operator_summary", answer),
        sections=[ResponseSection(**section) for section in result.get("answer_sections", [])],
        alerts=[AlertRecommendation(**alert) for alert in result.get("recommended_alerts", [])],
        next_steps=result.get("next_steps", []),
        query_type=result.get("query_type", "unknown"),
        conversation_id=conversation_id,
        used_sources=result.get("used_sources", []),
        context={
            "metrics_data": result.get("metrics_data", {}),
            "logs_data": result.get("logs_data", {}),
            "root_cause_data": result.get("root_cause_data", {}),
            "knowledge_data": result.get("knowledge_data", {}),
            "domain_data": result.get("domain_data", {}),
        },
    )
