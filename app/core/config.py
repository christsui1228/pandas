from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    # Database settings
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int = 5432  # Default PostgreSQL port
    DB_NAME: str

    # JWT settings
    SECRET_KEY: str = "your-default-secret-key-only-for-dev-please-change-in-env" # Default for local dev
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Add any other application-specific settings here
    # EXAMPLE: LOG_LEVEL: Optional[str] = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",                # Load .env file if variables not in environment
        env_file_encoding='utf-8',      # Specify encoding for .env file
        extra="ignore",                 # Ignore extra fields from .env or environment
        case_sensitive=False            # Environment variable names are case-insensitive
    )

settings = Settings() 