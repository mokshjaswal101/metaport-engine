import http
from fastapi import APIRouter, Query
from typing import List
import asyncio

# service
from .qc_service import QcService

from context_manager.context import context_user_data
from schema.base import GenericResponseModel
from utils.response_handler import build_api_response
from modules.qc.qc_schema import QCItemSchema

qc_router = APIRouter(prefix="/qc", tags=["Qc Management"])


# Qc Get endpoints
@qc_router.get(
    "/",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_qc_list():
    response: GenericResponseModel = await QcService.get_list()
    return build_api_response(response)


# Qc Post endpoints
@qc_router.post(
    "/add_qc",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def add_qc(qc_item: QCItemSchema):
    response: GenericResponseModel = await QcService.add_qc(qc_item=qc_item)
    return build_api_response(response)


# Qc Post endpoints
@qc_router.post(
    "/getqc",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def add_qc():
    response: GenericResponseModel = await QcService.get_qc()
    return build_api_response(response)
