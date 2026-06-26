import os

from pydantic import BaseModel, Field


class Settings(BaseModel):
    prometheus_url: str = Field(default_factory=lambda: os.getenv("PROMETHEUS_URL", "http://prometheus:9090"))
    loki_url: str = Field(default_factory=lambda: os.getenv("LOKI_URL", "http://loki:3100"))
    grafana_url: str = Field(default_factory=lambda: os.getenv("GRAFANA_URL", "http://grafana:3000"))
    sample_service_url: str = Field(default_factory=lambda: os.getenv("SAMPLE_SERVICE_URL", "http://sample-service:8080"))
    ollama_url: str = Field(default_factory=lambda: os.getenv("OLLAMA_URL", "http://ollama:11434"))
    ollama_model: str = Field(default_factory=lambda: os.getenv("OLLAMA_MODEL", "qwen2.5-coder:14b"))
    ollama_fallback_model: str = Field(
        default_factory=lambda: os.getenv("OLLAMA_FALLBACK_MODEL", os.getenv("OLLAMA_SECONDARY_MODEL", "deepseek-coder:6.7b"))
    )
    ollama_timeout_seconds: float = Field(default_factory=lambda: float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "120")))
    metrics_timeout_seconds: float = Field(default_factory=lambda: float(os.getenv("METRICS_TIMEOUT_SECONDS", "10")))
    logs_timeout_seconds: float = Field(default_factory=lambda: float(os.getenv("LOGS_TIMEOUT_SECONDS", "10")))


settings = Settings()
