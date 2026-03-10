"""Application settings and environment configuration."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed configuration sourced from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = Field(default="mongodb://localhost:27017", alias="MONGO_URI")
    mongo_db: str = Field(default="flipkart", alias="MONGO_DB")
    mongo_bronze_collection: str = Field(
        default="flipkart_reviews_bronze", alias="MONGO_BRONZE_COLLECTION"
    )

    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="flipkart", alias="POSTGRES_DB")
    postgres_user: str = Field(default="flipkart", alias="POSTGRES_USER")
    postgres_password: str = Field(default="flipkart", alias="POSTGRES_PASSWORD")

    request_timeout_seconds: int = Field(default=20, alias="REQUEST_TIMEOUT_SECONDS")
    request_sleep_seconds: float = Field(default=1.5, alias="REQUEST_SLEEP_SECONDS")
    request_max_retries: int = Field(default=5, alias="REQUEST_MAX_RETRIES")
    request_backoff_base: float = Field(default=1.5, alias="REQUEST_BACKOFF_BASE")

    embedding_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2", alias="EMBEDDING_MODEL"
    )
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    data_dir: str = Field(default="./data", alias="DATA_DIR")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
