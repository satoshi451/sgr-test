from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from text2sql_agent.config import settings
from text2sql_agent.services.categories import CategoryService
from text2sql_agent.services.metadata import MetadataService
from text2sql_agent.services.reasoning import ReasoningService

app = FastAPI(title="Text2SQL Agent (SGR)", version="0.1.0")

metadata_service = MetadataService(settings=settings)
category_service = CategoryService(metadata_service=metadata_service)
reasoning_service = ReasoningService(settings=settings, metadata_service=metadata_service)


class QueryRequest(BaseModel):
    query: str = Field(..., description="User message or desired question in natural language")
    context: dict = Field(default_factory=dict, description="Optional dialog context/state")
    refresh_schema: bool = Field(
        default=False, description="Force reload of schema metadata ignoring cache"
    )


class ExecuteRequest(BaseModel):
    sql: str
    limit: int | None = Field(default=50, ge=1, description="Preview row limit")


@app.get("/api/schema")
async def get_schema(refresh: bool = False):
    return metadata_service.describe_allowed_schemas(use_cache=not refresh)


@app.get("/api/categories")
async def get_categories(schema: str, table: str, column: str, limit: int = 20):
    return {
        "values": category_service.fetch_categories(
            schema=schema, table=table, column=column, limit=limit
        )
    }


@app.post("/api/query")
async def generate_query(payload: QueryRequest):
    schema = metadata_service.describe_allowed_schemas(use_cache=not payload.refresh_schema)
    return reasoning_service.generate_sql(payload.dict(), schema=schema)


@app.post("/api/execute")
async def execute_query(payload: ExecuteRequest):
    try:
        return metadata_service.execute_preview(sql=payload.sql, limit=payload.limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
