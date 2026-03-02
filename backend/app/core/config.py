# """
# Application Configuration
# Reads from .env file — never hardcode secrets in code
# """

# from pydantic_settings import BaseSettings
# from typing import Optional


# class Settings(BaseSettings):
#     # App
#     APP_NAME: str = "ERP Anomaly Detection Platform"
#     DEBUG: bool = False

#     # Database — PostgreSQL
#     DATABASE_URL: str = "postgresql://admin:password@localhost:5432/erp_anomaly_db"

#     # Security
#     SECRET_KEY: str = "change-this-to-a-random-secret-in-production"
#     ALGORITHM: str = "HS256"
#     ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

#     # File Upload
#     MAX_FILE_SIZE_MB: int = 50
#     UPLOAD_DIR: str = "./uploads"

#     # Anomaly Detection Thresholds (ERP/SAP specific)
#     ZSCORE_THRESHOLD: float = 3.0          # Flag if > 3 std deviations from mean
#     ISOLATION_FOREST_CONTAMINATION: float = 0.05  # Expect ~5% anomalies

#     class Config:
#         env_file = ".env"
#         case_sensitive = True


# settings = Settings()
"""
Application Configuration
Reads from .env file — never hardcode secrets in code
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # App
    APP_NAME: str = "ERP Anomaly Detection Platform"
    DEBUG: bool = False

    # Database — PostgreSQL
    DATABASE_URL: str = "postgresql://admin:password@localhost:5432/erp_anomaly_db"

    # Security
    SECRET_KEY: str = "change-this-to-a-random-secret-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # File Upload
    MAX_FILE_SIZE_MB: int = 50
    UPLOAD_DIR: str = "./uploads"

    # Anomaly Detection Thresholds (ERP/SAP specific)
    ZSCORE_THRESHOLD: float = 3.0          # Flag if > 3 std deviations from mean
    ISOLATION_FOREST_CONTAMINATION: float = 0.05  # Expect ~5% anomalies


    # Email Alerts (Phase 3)
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    ALERT_EMAIL: str = ""
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()

# Added in Phase 3 — Email Alerts
# Fill these in your .env file to enable automatic email alerts