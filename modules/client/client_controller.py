import http
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .client_schema import (
    ClientInsertModel,
    getClientFiltersModel,
    OtpVerified,
)

# utils
from utils.response_handler import build_api_response

# service
from .client_service import ClientService

# creating a client router
client_router = APIRouter(tags=["client"], prefix="/client")


@client_router.post(
    "/create", status_code=http.HTTPStatus.CREATED, response_model=GenericResponseModel
)
async def create_new_client(
    client_data: ClientInsertModel,
):
    response: GenericResponseModel = ClientService.create_client(
        client_data=client_data
    )
    return build_api_response(response)


@client_router.post(
    "/", status_code=http.HTTPStatus.CREATED, response_model=GenericResponseModel
)
async def get_all_clients(clientFilters: getClientFiltersModel):
    response: GenericResponseModel = ClientService.get_all_clients(
        clientFilters=clientFilters
    )
    return build_api_response(response)
