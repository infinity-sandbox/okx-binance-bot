from fastapi import APIRouter
from app.api.api_v1.handlers import trade

router = APIRouter()

router.include_router(trade.api, prefix='/trade', tags=["trade"])