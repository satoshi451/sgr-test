from __future__ import annotations

from typing import Any, Dict, List

import trino

from text2sql_agent.config import Settings, TrinoWhitelist


class MetadataService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _connect(self) -> trino.dbapi.Connection:
        return trino.dbapi.connect(
            host=self.settings.trino_host,
            port=self.settings.trino_port,
            user=self.settings.trino_user,
            catalog=self.settings.trino_catalog,
            http_scheme="https" if self.settings.trino_ssl else "http",
            verify=self.settings.trino_ssl,
        )

    def describe_allowed_schemas(self) -> Dict[str, List[Dict[str, Any]]]:
        whitelist = self.settings.whitelist
        schemas: List[Dict[str, Any]] = []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SHOW SCHEMAS")
            rows = cur.fetchall()
        for row in rows:
            schema = row[0]
            if whitelist.is_allowed(schema):
                schemas.append({"schema": schema, "tables": []})
        return {"schemas": schemas}

    def fetch_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        whitelist = self.settings.whitelist
        if not whitelist.is_allowed(schema, table):
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(f"SHOW COLUMNS FROM {schema}.{table}")
            rows = cur.fetchall()
        return [
            {"column": row[0], "type": row[1], "extra": row[2:]}
            for row in rows
        ]

    def execute_preview(self, sql: str, limit: int | None = 50) -> Dict[str, Any]:
        if limit and "limit" not in sql.lower():
            sql = f"{sql.rstrip(';')} LIMIT {limit}"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]
        return {"columns": columns, "rows": rows}

    def whitelist(self) -> TrinoWhitelist:
        return self.settings.whitelist
