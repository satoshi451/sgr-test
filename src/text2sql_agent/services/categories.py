from __future__ import annotations

from typing import List

from text2sql_agent.services.metadata import MetadataService


class CategoryService:
    def __init__(self, metadata_service: MetadataService) -> None:
        self.metadata_service = metadata_service

    def fetch_categories(self, schema: str, table: str, column: str, limit: int = 20) -> List[str]:
        if not self.metadata_service.whitelist().is_allowed(schema, table):
            return []
        sql = f"SELECT DISTINCT {column} FROM {schema}.{table} LIMIT {limit}"
        result = self.metadata_service.execute_preview(sql, limit=limit)
        values = [row[0] for row in result.get("rows", []) if row]
        return [value for value in values if value is not None]
