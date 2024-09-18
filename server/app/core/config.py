from typing import List 
from decouple import config
from logs.loggers.logger import logger_config
from pydantic_settings import BaseSettings
from pydantic import AnyHttpUrl
logger = logger_config(__name__)
from utils.version import get_version_and_build
version, build = get_version_and_build()

class Settings(BaseSettings):
    logger.info("Loading configs...")
    VERSION: str = version
    BUILD: str = build
    API_V1_STR: str = "/api/v1"
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = [
                                              "http://localhost:3000",
                                              "http://0.0.0.0:3000",
                                            ]
    PROJECT_NAME: str = "okx-binance-bot"
    MYSQL_DB_USER: str = config("MYSQL_DB_USER", cast=str)
    MYSQL_DB_PASSWORD: str = config("MYSQL_DB_PASSWORD", cast=str)
    MYSQL_DB_HOST: str = config("MYSQL_DB_HOST", cast=str)
    MYSQL_DB: str = config("MYSQL_DB", cast=str)
    MYSQL_DB_PORT: str = config("MYSQL_DB_PORT", cast=str)
    MYSQL_DB_URL: str = f"mysql+mysqlconnector://{MYSQL_DB_USER}:{MYSQL_DB_PASSWORD}@{MYSQL_DB_HOST}:{MYSQL_DB_PORT}/{MYSQL_DB}"

    class Config:
        case_sensitive = True
        env_file = ".env"
        
settings = Settings()
