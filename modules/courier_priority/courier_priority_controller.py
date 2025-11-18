import http
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer

# schema
from schema.base import GenericResponseModel
from modules.courier_priority.courier_priority_schema import (
    addClientMetaOptionsRequest,
    addRulesAndCourierPriority,
    Courier_Rules_status,
    Update_Rule_Model,
    Courier_Deactivate_Model,  # SERCICE DEACTIVATE (OFF)
)


# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .courier_priority_service import CourierPriorityService
import asyncio

# creating a client router
courier_allocation_router = APIRouter(
    tags=["courier_allocation"], prefix="/courier-priority"
)
security = HTTPBearer()


@courier_allocation_router.post(
    "/add-priority",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def courier_Ordering(reording: addClientMetaOptionsRequest):
    try:
        response = CourierPriorityService.add_courier_priority(reording)
        # return response
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/add-courier-rules",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def add_rules_courier_priority(addRule: addRulesAndCourierPriority):
    try:
        response = CourierPriorityService.add_rules_courier_priority(addRule)
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/all-courier-rules",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def all_courier_rules():
    try:
        response = CourierPriorityService.all_courier_rules_service()
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/update-rule-order",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def update_rule_order(update_rule_order: Update_Rule_Model):
    try:
        print("success trigger")
        response = CourierPriorityService.update_rule_order_service(update_rule_order)
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/courier-rules-status-update",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def courier_roles_status_update(status_update: Courier_Rules_status):
    try:
        response = CourierPriorityService.courier_roles_update(status_update)
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/courier-list",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def courier_Ordering_get():
    try:
        response: GenericResponseModel = (
            CourierPriorityService.get_courier_priority_list()
        )
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/courier-deactivate",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def courier_deactivate(courier_method: Courier_Deactivate_Model):
    try:
        response: GenericResponseModel = (
            CourierPriorityService.courier_deactivate_service(
                courier_method=courier_method
            )
        )
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/test",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def courier_deactivate():
    try:
        response: GenericResponseModel = CourierPriorityService.cheapest()
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@courier_allocation_router.post(
    "/courier-config-settings",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def courier_config_settings():
    try:
        response: GenericResponseModel = (
            CourierPriorityService.courier_config_settings()
        )
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )
