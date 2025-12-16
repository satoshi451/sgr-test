from __future__ import annotations

from fastapi import FastAPI

from text2sql_agent.config import settings
from text2sql_agent.services.categories import CategoryService
from text2sql_agent.services.metadata import MetadataService
from text2sql_agent.services.reasoning import ReasoningService

app = FastAPI(title="Text2SQL Agent (SGR)", version="0.1.0")

metadata_service = MetadataService(settings=settings)
category_service = CategoryService(metadata_service=metadata_service)
reasoning_service = ReasoningService(settings=settings, metadata_service=metadata_service)


@app.get("/api/schema")
async def get_schema():
    return metadata_service.describe_allowed_schemas()


@app.get("/api/categories")
async def get_categories(schema: str, table: str, column: str, limit: int = 20):
    return {
        "values": category_service.fetch_categories(
            schema=schema, table=table, column=column, limit=limit
        )
    }


@app.post("/api/query")
async def generate_query(payload: dict):
    return reasoning_service.generate_sql(payload)


@app.post("/api/execute")
async def execute_query(payload: dict):
    sql = payload.get("sql", "")
    return metadata_service.execute_preview(sql)
