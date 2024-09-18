import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from logs.loggers.logger import logger_config
from app.models.user_model import User
from app.api.api_v1.router import router
from utils.console.banner import run_banner
from pathlib import Path
from app.services.database import Base, create_database_if_not_exists, create_tables_if_not_exist

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f'{settings.API_V1_STR}/openapi.json',
)

run_banner(settings.VERSION, settings.BUILD)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def serve_frontend():
    return JSONResponse(
                content={
                    "message": "OKX Binance Bot. Trading in the Zone!",
                }
            )


async def setup():
    # Create the engine
    engine = create_engine(settings.MYSQL_DB_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal, engine


@app.on_event("startup")
async def app_init():
    """
    Initialize crucial application services
    """
    """
    Initialize the application by creating the database and tables if they don't exist.
    """
    create_database_if_not_exists()
    create_tables_if_not_exist()
    
app.include_router(router, prefix=settings.API_V1_STR)
