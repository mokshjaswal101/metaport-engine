import http
from fastapi import (
    APIRouter,
    Request,
    Query,
    Depends,
    HTTPException,
    File,
    UploadFile,
    Form,
)
import os

from typing import Any, List, Dict
from datetime import datetime
from fastapi.responses import RedirectResponse

# schema
from schema.base import GenericResponseModel
from modules.discrepancie.discrepancie_schema import (
    upload_rate_discrepancie_model,
    Accept_Description_Model,
    Accept_Bulk_Description_Model,
    Dispute_Model,
    Bulk_Dispute_Model,
    Status_Model_Schema,
    Status_Model,
    Accept_Dispute_Model,
    view_History_Schema,
)

# utils
from utils.response_handler import build_api_response

# services
from .discrepancie_service import DiscrepancieService

discrepancie_router = APIRouter(prefix="/discrepancie")


# CLIENT SIDE ROUTES
# create a new order
@discrepancie_router.post(
    "/rateUpload",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def upload_rate(
    upload_rate: List[upload_rate_discrepancie_model],
):
    try:
        response: GenericResponseModel = DiscrepancieService.upload_rate(
            upload_rate=upload_rate
        )
        return response
        # return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


# create a new order
@discrepancie_router.post(
    "/ratelist",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def ratelist(tab_action: Status_Model):
    try:
        response: GenericResponseModel = DiscrepancieService.all_ratelist(
            tab_action=tab_action
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


# create view history
@discrepancie_router.post(
    "/view-history",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def view_history(
    view_history: view_History_Schema,
):
    try:
        response: GenericResponseModel = DiscrepancieService.view_history(
            view_history=view_history
        )
        return response
        # return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


# Single Accept
@discrepancie_router.post(
    "/acceptDescription",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def accept_Description(accept_description: Accept_Description_Model):
    try:
        response: GenericResponseModel = DiscrepancieService.accept_Description(
            accept_description=accept_description
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


# Bulk Accept
@discrepancie_router.post(
    "/acceptBulkDescription",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def accept_Bulk_Description(
    accept_bulk_description: Accept_Bulk_Description_Model,
):
    try:
        print("**Action Trigger**")
        response: GenericResponseModel = DiscrepancieService.accept_bulk_Description(
            accept_bulk_description=accept_bulk_description
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


@discrepancie_router.post(
    "/disputefileupload",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def dispute_file_upload(file: UploadFile = File(...), category: str = Form(...)):
    try:
        response: GenericResponseModel = DiscrepancieService.dispute_file_upload(
            file, category
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the dispute.",
            )
        )


# Single Dispute
@discrepancie_router.post(
    "/dispute",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def dispute(dispute: Dispute_Model):
    try:
        response: GenericResponseModel = DiscrepancieService.dispute(dispute=dispute)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the dispute.",
            )
        )


# Bulk Dispute
@discrepancie_router.post(
    "/bulkdispute",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def bulk_dispute(bulk_dispute: Bulk_Dispute_Model):
    try:
        response: GenericResponseModel = DiscrepancieService.bulk_dispute(
            bulk_dispute=bulk_dispute
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the dispute.",
            )
        )


# Report
@discrepancie_router.post(
    "/report",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def report(status: Status_Model_Schema):
    try:
        response: GenericResponseModel = DiscrepancieService.generate_report(
            status=status
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the report.",
            )
        )
