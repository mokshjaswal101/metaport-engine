import http
from fastapi import APIRouter, Request
from typing import Any, List, Dict
from datetime import datetime


# schema
from schema.base import GenericResponseModel

# modules
from modules.ndr.ndr_schema import (
    Ndr_filters,
    Ndr_reattempt_escalate,
    Ndr_status_update,
    Bulk_Ndr_reattempt_escalate,
)

# utils
from utils.response_handler import build_api_response

# services
from .ndr_service import NdrService


# Creating the router for orders
ndr_router = APIRouter(prefix="/ndr", tags=["ndr"])


@ndr_router.get(
    "/health-check",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
def ndr_health_check():
    """
    ✅ NEW: Health check endpoint for NDR system
    """
    try:
        response: GenericResponseModel = NdrService.health_check_ndr_system()
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred during NDR health check.",
            )
        )


@ndr_router.post(
    "/backfill",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
def ndr_backfill_data():
    """
    🔄 Backfill NDR data for existing shipments

    This endpoint processes existing orders that are in NDR status but don't have
    proper NDR records. It will:
    1. Find all orders with current status = NDR
    2. Analyze their tracking history to calculate attempt counts
    3. Create proper NDR records with correct attempt counts
    4. Create NDR history entries

    Use with caution - this is a data migration operation.
    """
    try:
        from modules.shipment.shipment_service import ShipmentService

        # Call the backfill method
        result = ShipmentService.backfill_ndr_data()

        if result["status"]:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data=result["stats"],
                    message=result["message"],
                    status=True,
                )
            )
        else:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    data=result["stats"],
                    message=result["message"],
                    status=False,
                )
            )

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred during NDR data backfill.",
                status=False,
            )
        )


@ndr_router.post(
    "/",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def get_all_ndr(
    ndr_filters: Ndr_filters,
):
    try:
        response: GenericResponseModel = NdrService.get_all_ndr(ndr_filters=ndr_filters)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@ndr_router.post(
    "/export",
    status_code=http.HTTPStatus.CREATED,
)
def get_all_ndr(
    ndr_filters: Ndr_filters,
):
    try:
        response: GenericResponseModel = NdrService.export_all_ndr(
            ndr_filters=ndr_filters
        )
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@ndr_router.post(
    "/bulkstatus",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def bulkstatus(
    bulkstatus: Bulk_Ndr_reattempt_escalate,
):
    try:
        # print("bulkstatus")
        response: GenericResponseModel = NdrService.bulk_ndr_status_change(
            bulkstatus=bulkstatus
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


# reattemptescalate
@ndr_router.post(
    "/reattemptescalate",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def re_attempt_escalate(
    ndr_reattempt_escalate: Ndr_reattempt_escalate,
):
    try:
        response: GenericResponseModel = NdrService.ndr_reattempt_escalate(
            ndr_reattempt_escalate=ndr_reattempt_escalate
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


@ndr_router.post(
    "/bulkstatus",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def bulkstatus(
    bulkstatus: Bulk_Ndr_reattempt_escalate,
):
    try:
        # print("bulkstatus")
        response: GenericResponseModel = NdrService.bulk_ndr_status_change(
            bulkstatus=bulkstatus
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


# status
@ndr_router.post(
    "/status",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
def re_attempt_escalate(
    ndr_status_update: Ndr_status_update,
):
    try:
        response: GenericResponseModel = NdrService.ndr_status_update(
            ndr_status_update=ndr_status_update
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
