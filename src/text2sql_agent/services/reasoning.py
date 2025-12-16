from __future__ import annotations

from importlib.util import find_spec
from typing import Any, Dict

from text2sql_agent.config import Settings
from text2sql_agent.services.metadata import MetadataService

if find_spec("sgr_agent_core"):
    from sgr_agent_core import Client as SgrClient
else:  # pragma: no cover - optional dependency at this stage
    SgrClient = None


class ReasoningService:
    def __init__(self, settings: Settings, metadata_service: MetadataService) -> None:
        self.settings = settings
        self.metadata_service = metadata_service
        self.client = (
            SgrClient(base_url=settings.llm.base_url, api_key=settings.llm.api_key)
            if SgrClient
            else None
        )

    def generate_sql(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        user_query = payload.get("query") or payload.get("message") or ""
        context = payload.get("context", {})
        schema_info = self.metadata_service.describe_allowed_schemas()

        if self.client:
            response = self.client.run(
                task="text2sql",
                input={"query": user_query, "schema": schema_info, "context": context},
                model=self.settings.llm.model,
                temperature=self.settings.llm.temperature,
                max_tokens=self.settings.llm.max_tokens,
            )
            return {"sql": response.get("sql"), "steps": response.get("steps", [])}

        return {
            "sql": None,
            "steps": [
                {
                    "type": "info",
                    "message": "LLM client is not configured; provide TEXT2SQL_LLM_BASE_URL and TEXT2SQL_LLM_MODEL.",
                }
            ],
            "schema": schema_info,
        }
