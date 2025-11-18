import http
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi import Body

from fastapi import APIRouter, Depends

from fastapi import Request
from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response

# service
from .byoc_service import ManageCourierForClient

# schema
from .byoc_schema import courier_Status, CourierFilterRequest

from .byoc_schema import (
    CourierAssignRequest,
    courier_Status,
    GetSingleContract,
    SingleRateUploadModel,
)

# creating a client router
byoc_router = APIRouter(tags=["byoc"])


@byoc_router.post(
    "/byoc/courierforclient",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_courier_for_client(
    payload: CourierFilterRequest = Body(...),
):
    response: GenericResponseModel = ManageCourierForClient.get_courier_for_client(
        payload
    )
    return build_api_response(response)


@byoc_router.post(
    "/byoc/courierStatusChange",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def courier_status_change(courier_status: courier_Status):
    response: GenericResponseModel = ManageCourierForClient.courier_status_change(
        courier_status
    )
    return build_api_response(response)


@byoc_router.post(
    "/byoc/exportcouriers",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def courier_status_change(courier: CourierFilterRequest):
    response: GenericResponseModel = ManageCourierForClient.export_Couriers(courier)
    return build_api_response(response)


@byoc_router.get(
    "/byoc/courier-stats",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_courier_stats():
    response = ManageCourierForClient.get_courier_stats()
    return build_api_response(response)


@byoc_router.get(
    "/byoc/all_contracts",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def all_contracts():
    print("**Hello abcd**")
    response: GenericResponseModel = ManageCourierForClient.all_contracts()
    return build_api_response(response)


@byoc_router.post(
    "/byoc/add_contracts",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def add_contracts(add_courier_payload: CourierAssignRequest):
    response: GenericResponseModel = await ManageCourierForClient.add_contracts(
        add_courier_payload
    )
    return build_api_response(response)


@byoc_router.post(
    "/byoc/get_contract_by_id",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_contract_by_id(get_single_courier: GetSingleContract):
    response: GenericResponseModel = ManageCourierForClient.get_contract_by_id(
        get_single_courier
    )
    return build_api_response(response)


@byoc_router.post(
    "/byoc/single_contract_rate",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def upload_single_contract_rate(Single_rate_upload: SingleRateUploadModel):
    response: GenericResponseModel = ManageCourierForClient.single_contract_rate(
        Single_rate_upload
    )
    return build_api_response(response)
