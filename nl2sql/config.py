
import os
from dotenv import load_dotenv

load_dotenv()


class Config:

    # LLM API Keys (set at least one)
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
    
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "auto")

    # PostgreSQL
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "postgres")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # App settings
    MAX_CORRECTION_ATTEMPTS: int = int(os.getenv("MAX_CORRECTION_ATTEMPTS", "3"))
    MAX_ROWS_RETURN: int = int(os.getenv("MAX_ROWS_RETURN", "100"))
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))

    @classmethod
    def get_db_connection_string(cls) -> str:
        return (
            f"host={cls.DB_HOST} port={cls.DB_PORT} "
            f"dbname={cls.DB_NAME} user={cls.DB_USER} "
            f"password={cls.DB_PASSWORD}"
        )

    @classmethod
    def get_llm_provider(cls) -> str:
        if cls.LLM_PROVIDER and cls.LLM_PROVIDER != "auto":
            return cls.LLM_PROVIDER.lower()
        # Auto-detect: prefer Groq if key is set
        if cls.GROQ_API_KEY and cls.GROQ_API_KEY not in ("", "your_groq_api_key_here"):
            return "groq"
        if cls.GEMINI_API_KEY and cls.GEMINI_API_KEY not in ("", "your_gemini_api_key_here"):
            return "gemini"
        return "none"

    @classmethod
    def validate(cls) -> list[str]:
        errors = []
        has_gemini = cls.GEMINI_API_KEY and cls.GEMINI_API_KEY != "your_gemini_api_key_here"
        has_groq = cls.GROQ_API_KEY and cls.GROQ_API_KEY not in ("", "your_groq_api_key_here")
        if not has_gemini and not has_groq:
            errors.append("No LLM API key set. Set GROQ_API_KEY or GEMINI_API_KEY in .env")
        if not cls.DB_NAME or cls.DB_NAME == "your_database_name":
            errors.append("DB_NAME is not set")
        if not cls.DB_USER or cls.DB_USER == "your_username":
            errors.append("DB_USER is not set")
        return errors
