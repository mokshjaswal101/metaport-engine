import http
from typing import Optional
from fastapi import APIRouter, Query

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .pickup_location_schema import PickupLocationInsertModel, PickupLocationUpdateModel

# utils
from utils.response_handler import build_api_response

# service
from .pickup_location_service import PickupLocationService

# creating a client router
pickup_router = APIRouter(tags=["pickup location"], prefix="/pickuplocation")


@pickup_router.post(
    "/add",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_new_location(
    pickup_location_data: PickupLocationInsertModel,
):
    """Create a new pickup location. First location is automatically set as default."""
    response: GenericResponseModel = PickupLocationService.create_pickup_location(
        pickup_location_data=pickup_location_data
    )
    return build_api_response(response)


@pickup_router.post(
    "/set-default/{location_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def set_default_location(location_id: str):
    """Set a pickup location as the default"""
    response: GenericResponseModel = PickupLocationService.set_default_location(
        pickup_location_id=location_id
    )
    return build_api_response(response)


@pickup_router.put(
    "/toggle-status/{location_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def toggle_location_status(location_id: str):
    """Toggle the active status of a pickup location (enable/disable)"""
    response: GenericResponseModel = (
        PickupLocationService.toggle_location_active_status(
            pickup_location_id=location_id
        )
    )
    return build_api_response(response)


@pickup_router.put(
    "/{location_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def update_pickup_location(
    location_id: str,
    update_data: PickupLocationUpdateModel,
):
    """Update a pickup location"""
    response: GenericResponseModel = PickupLocationService.update_pickup_location(
        pickup_location_id=location_id,
        update_data=update_data,
    )
    return build_api_response(response)


@pickup_router.get(
    "/",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_locations(
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(
        default=10, ge=1, le=100, description="Number of items per page"
    ),
    search: Optional[str] = Query(
        default=None,
        description="Search by name, code, contact, address, city, pincode",
    ),
):
    """Get all pickup locations with orders count (paginated)"""
    response: GenericResponseModel = PickupLocationService.get_pickup_locations(
        page=page,
        page_size=page_size,
        search=search,
    )
    return build_api_response(response)


@pickup_router.delete(
    "/{location_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def delete_pickup_location(location_id: str):
    """Delete a pickup location (soft delete). Fails if orders exist."""
    response: GenericResponseModel = PickupLocationService.delete_location(
        pickup_location_id=location_id
    )
    return build_api_response(response)
