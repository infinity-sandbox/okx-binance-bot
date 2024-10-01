import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.core.config import settings
from logs.loggers.logger import logger_config
from app.api.api_v1.router import router
from utils.console.banner import run_banner


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f'{settings.API_V1_STR}/openapi.json',
)

run_banner(settings.VERSION, settings.BUILD)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def serve_frontend():
    return JSONResponse(
                content={
                    "message": "okx binance bot, trading in the zone!",
                }
            )

@app.on_event("startup")
async def app_init():
    """
    Initialize crucial application services
    """
    pass
    
app.include_router(router, prefix=settings.API_V1_STR)
