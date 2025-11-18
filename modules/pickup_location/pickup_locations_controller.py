import http
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from fastapi import APIRouter, Depends

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .pickup_location_schema import PickupLocationInsertModel

# utils
from utils.response_handler import build_api_response

# service
from .pickup_location_service import PickupLocationService

# creating a client router
pickup_router = APIRouter(tags=["pickup location"])


@pickup_router.post(
    "/pickuplocation/add",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_new_location(
    pickup_location_data: PickupLocationInsertModel,
):
    response: GenericResponseModel = PickupLocationService.create_pickup_location(
        pickup_location_data=pickup_location_data
    )
    return build_api_response(response)


@pickup_router.post(
    "/dev/pickuplocation/add",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_new_location(
    pickup_location_data: PickupLocationInsertModel,
):
    response: GenericResponseModel = PickupLocationService.create_pickup_location(
        pickup_location_data=pickup_location_data
    )
    return build_api_response(response)


@pickup_router.post(
    "/pickuplocation/set-default/{location_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def set_default_location(location_id: str):
    response: GenericResponseModel = PickupLocationService.set_default_location(
        pickup_location_id=location_id
    )
    return build_api_response(response)


@pickup_router.put(
    "/pickuplocation/status/{location_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def set_default_location(location_id: str):
    response: GenericResponseModel = PickupLocationService.set_default_location(
        pickup_location_id=location_id
    )
    return build_api_response(response)


@pickup_router.get(
    "/pickuplocation/",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_locations():
    response: GenericResponseModel = PickupLocationService.get_pickup_locations()
    return build_api_response(response)


@pickup_router.delete(
    "/pickuplocation/{location_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def delete_pickup_location(location_id: str):
    response: GenericResponseModel = PickupLocationService.delete_location(
        pickup_location_id=location_id
    )
    return build_api_response(response)
