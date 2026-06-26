from __future__ import annotations

from pathlib import Path


def _resolve_docs_dir() -> Path:
    candidates = [
        Path(__file__).resolve().parents[1] / "docs",
        Path(__file__).resolve().parents[2] / "docs",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


DOCS_DIR = _resolve_docs_dir()


def _read_doc(name: str) -> str:
    path = DOCS_DIR / name
    return path.read_text(encoding="utf-8")


def query_knowledge(question: str) -> dict[str, str]:
    lowered = question.lower()
    if any(token in lowered for token in ["architecture", "design", "stack", "system"]):
        source = "architecture.md"
        answer = _read_doc(source)
    elif any(token in lowered for token in ["change", "changed", "today", "day"]):
        source = "change-log.md"
        answer = _read_doc(source)
    else:
        source = "architecture.md"
        answer = _read_doc(source)
    return {"source": source, "content": answer}
