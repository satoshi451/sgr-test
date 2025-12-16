from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import trino

from text2sql_agent.config import Settings, TrinoWhitelist
from text2sql_agent.utils.cache import JsonCache


class MetadataService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        cache_dir = Path(settings.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.schema_cache = JsonCache(cache_dir / "schema.json")

    def _connect(self) -> trino.dbapi.Connection:
        return trino.dbapi.connect(
            host=self.settings.trino_host,
            port=self.settings.trino_port,
            user=self.settings.trino_user,
            catalog=self.settings.trino_catalog,
            http_scheme="https" if self.settings.trino_ssl else "http",
            verify=self.settings.trino_ssl,
        )

    def describe_allowed_schemas(self, use_cache: bool = True) -> Dict[str, Any]:
        cached = self.schema_cache.read()
        if use_cache and cached:
            return cached

        whitelist = self.settings.whitelist
        schemas: List[Dict[str, Any]] = []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SHOW SCHEMAS")
            schema_rows = cur.fetchall()
            for schema_row in schema_rows:
                schema = schema_row[0]
                if not whitelist.is_allowed(schema):
                    continue

                cur.execute(f"SHOW TABLES FROM {schema}")
                table_rows = cur.fetchall()
                tables: List[Dict[str, Any]] = []
                for table_row in table_rows:
                    table = table_row[0]
                    if not whitelist.is_allowed(schema, table):
                        continue
                    columns = self.fetch_table_columns(schema, table, cursor=cur)
                    tables.append({"table": table, "columns": columns})

                schemas.append({"schema": schema, "tables": tables})

        result = {"schemas": schemas}
        self.schema_cache.write(result)
        return result

    def refresh_schema_cache(self) -> Dict[str, Any]:
        return self.describe_allowed_schemas(use_cache=False)

    def fetch_table_columns(
        self, schema: str, table: str, cursor: Optional[trino.dbapi.Cursor] = None
    ) -> List[Dict[str, Any]]:
        whitelist = self.settings.whitelist
        if not whitelist.is_allowed(schema, table):
            return []
        external_cursor = cursor is not None
        if cursor is None:
            conn = self._connect()
            cursor = conn.cursor()
        try:
            cursor.execute(f"SHOW COLUMNS FROM {schema}.{table}")
            rows = cursor.fetchall()
        finally:
            if not external_cursor:
                cursor.close()
                conn.close()
        return [
            {"column": row[0], "type": row[1], "extra": row[2:]}
            for row in rows
        ]

    def execute_preview(self, sql: str, limit: int | None = 50) -> Dict[str, Any]:
        sql_normalized = sql.strip().lower()
        forbidden = ("insert", "update", "delete", "drop", "alter", "create")
        if any(sql_normalized.startswith(keyword) for keyword in forbidden):
            raise ValueError("Only read-only SELECT queries are allowed")

        if limit and "limit" not in sql_normalized:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]
        return {"columns": columns, "rows": rows}

    def whitelist(self) -> TrinoWhitelist:
        return self.settings.whitelist
