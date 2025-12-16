from __future__ import annotations

from typing import List, Optional, Sequence

from pydantic import BaseModel, BaseSettings, Field, HttpUrl, validator


class TrinoWhitelist(BaseModel):
    schemas: Sequence[str] = Field(default_factory=list)
    tables: Sequence[str] = Field(default_factory=list)

    def is_allowed(self, schema: str, table: str | None = None) -> bool:
        if schema not in self.schemas:
            return False
        if table is None:
            return True
        if not self.tables:
            return True
        return f"{schema}.{table}" in self.tables or table in self.tables


class LLMConfig(BaseModel):
    base_url: HttpUrl
    model: str
    api_key: Optional[str] = None
    temperature: float = 0.2
    max_tokens: int = 1200


class Settings(BaseSettings):
    trino_host: str = "localhost"
    trino_port: int = 8080
    trino_user: str = "text2sql"
    trino_catalog: str = "hive"
    trino_ssl: bool = False
    trino_timeout: int = 30

    whitelist: TrinoWhitelist = Field(
        default_factory=lambda: TrinoWhitelist(schemas=["default"], tables=[])
    )

    llm: LLMConfig = Field(
        default_factory=lambda: LLMConfig(
            base_url="https://api.example.llm/v1", model="demo-model"
        )
    )

    cache_dir: str = ".cache"

    class Config:
        env_prefix = "TEXT2SQL_"
        case_sensitive = False

    @validator("trino_port")
    def _validate_port(cls, value: int) -> int:
        if not 0 < value < 65536:
            raise ValueError("trino_port must be between 1 and 65535")
        return value


settings = Settings()
