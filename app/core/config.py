from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    All sensitive values must come from environment variables.
    No defaults are provided on purpose.
    """

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    app_name: str = Field(...)
    environment: str = Field(...)

    database_url: str = Field(..., description="PostgreSQL DSN, e.g. postgresql+asyncpg://user:pass@host:5432/db")
    redis_url: str = Field(..., description="Redis URL, e.g. redis://redis:6379/0")

    jwt_secret_key: str = Field(...)
    jwt_algorithm: str = Field(...)
    jwt_access_token_expire_minutes: int = Field(...)


@lru_cache
def get_settings() -> Settings:
    return Settings()

