import asyncio
import json
from fastapi import APIRouter, HTTPException, Header, status
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Depends, FastAPI
from logs.loggers.logger import logger_config
logger = logger_config(__name__)
from fastapi import APIRouter
from typing import List, Dict, Optional
from fastapi import WebSocket, WebSocketDisconnect, APIRouter, Header, HTTPException, status

api = APIRouter()

@api.post("/trade")
async def forecast(request: str):
    return {"message": f"Trading is not available at the moment. {request}"}

