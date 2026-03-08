from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    app_name: str = "art-activity-collection"
    app_env: str = "dev"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_db: str = "art_activity_collection"
    mysql_dsn_env: str | None = Field(default=None, alias="MYSQL_DSN")

    api_allowed_origins: str = "http://localhost:3000,http://127.0.0.1:3000"

    auth_enabled: bool = False
    auth_require_all_requests: bool = False
    auth_issuer: str | None = None
    auth_audience: str | None = None
    auth_jwks_url: str | None = None

    redis_url: str | None = None
    rate_limit_guest_per_minute: int = 30
    rate_limit_guest_per_day: int = 500
    rate_limit_user_per_minute: int = 120
    rate_limit_user_per_day: int = 5000

    log_level: str = "INFO"

    llm_enabled: bool = False
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o-mini"
    llm_api_key: str | None = None

    # Crawler/runtime helper variables (primarily used by scripts).
    crawler_alert_webhook_url: str | None = None
    crawler_config_path: str | None = None
    crawler_batch_id: str | None = None
    rawhtml_base_url: str | None = None

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
    )

    @property
    def mysql_host_resolved(self) -> str:
        host = (self.mysql_host or "").strip()
        if host.lower() in {"localhost", "::1", "[::1]"}:
            return "127.0.0.1"
        return host

    @property
    def mysql_dsn(self) -> str:
        if self.mysql_dsn_env:
            return self.mysql_dsn_env
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host_resolved}:{self.mysql_port}/{self.mysql_db}"
        )

    @property
    def api_allowed_origins_list(self) -> list[str]:
        parsed = [value.strip() for value in self.api_allowed_origins.split(",")]
        origins = [value for value in parsed if value]
        if origins:
            return origins
        return ["http://localhost:3000", "http://127.0.0.1:3000"]


settings = Settings()
