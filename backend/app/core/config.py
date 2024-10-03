from typing import List
from pydantic_settings import BaseSettings
from pydantic import AnyHttpUrl
from utils.version import get_version_and_build
version, build = get_version_and_build()

class Settings(BaseSettings):
    VERSION: str = version
    BUILD: str = build
    API_V1_STR: str = "/api/v1"
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = [
                                              "http://localhost:3000",
                                              "http://0.0.0.0:3000",
                                            ]
    PROJECT_NAME: str = "okx-binance-bot"
    CONFIG_FILE: str = "config.yml"
    
    class Config:
        case_sensitive = True
        
settings = Settings()
