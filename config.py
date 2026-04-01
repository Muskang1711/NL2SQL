from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    groq_api_key: str
    gemini_api_key: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_schema: str = "public"
    log_level: str = "INFO"
    sample_size: int = 500
    max_unique_values: int = 50
    groq_model: str = "meta-llama/llama-prompt-guard-2-86m"
    gemini_model: str = "gemini-2.0-flash"
    jaccard_sample_size: int = 100
    fk_match_threshold: float = 0.95

    # NL2SQL settings
    llm_provider: str = "auto"
    max_correction_attempts: int = 3
    max_rows_return: int = 100
    app_port: int = 8000

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()