"""
Reports Controller

API endpoints for report generation and management.
"""

import http
from typing import Optional
from fastapi import APIRouter, Query

from context_manager.context import build_request_context

from schema.base import GenericResponseModel
from utils.response_handler import build_api_response

from .reports_schema import GenerateReportRequest
from .reports_service import ReportsService


# Create router
reports_router = APIRouter(tags=["reports"])


@reports_router.post(
    "/reports/generate",
    status_code=http.HTTPStatus.ACCEPTED,
    response_model=GenericResponseModel,
)
async def generate_report(request: GenerateReportRequest):
    """
    Queue a new report generation job.

    The report will be generated in the background. Check the Downloads
    section in the Reports module for progress and download.

    Returns:
        Job ID and confirmation message
    """
    response: GenericResponseModel = ReportsService.generate_report(request=request)
    return build_api_response(response)


@reports_router.get(
    "/reports/downloads",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_downloads(
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(
        default=10, ge=1, le=100, description="Number of items per page"
    ),
    status: Optional[str] = Query(
        default=None,
        description="Filter by status (pending, processing, completed, failed, cancelled)",
    ),
):
    """
    Get paginated list of report downloads for the current client.

    Returns:
        List of downloads with pagination info
    """
    response: GenericResponseModel = ReportsService.get_downloads(
        page=page,
        page_size=page_size,
        status=status,
    )
    return build_api_response(response)


@reports_router.get(
    "/reports/download/{job_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_download_url(job_id: int):
    """
    Get a presigned download URL for a completed report.

    The URL is valid for 15 minutes.

    Args:
        job_id: ID of the report job

    Returns:
        Presigned download URL and file name
    """
    response: GenericResponseModel = ReportsService.get_download_url(job_id=job_id)
    return build_api_response(response)


@reports_router.post(
    "/reports/retry/{job_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def retry_report(job_id: int):
    """
    Retry a failed report generation.

    Args:
        job_id: ID of the failed report job

    Returns:
        Confirmation of retry
    """
    response: GenericResponseModel = ReportsService.retry_report(job_id=job_id)
    return build_api_response(response)


@reports_router.post(
    "/reports/cancel/{job_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def cancel_report(job_id: int):
    """
    Cancel a pending report generation.

    Only pending reports can be cancelled.

    Args:
        job_id: ID of the report job to cancel

    Returns:
        Confirmation of cancellation
    """
    response: GenericResponseModel = ReportsService.cancel_report(job_id=job_id)
    return build_api_response(response)


@reports_router.get(
    "/reports/types",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_report_types():
    """
    Get list of available report types.

    Returns:
        List of available report types with descriptions
    """
    response: GenericResponseModel = ReportsService.get_report_types()
    return build_api_response(response)


@reports_router.get(
    "/reports/status/{job_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_report_status(job_id: int):
    """
    Get the status of a specific report job.

    Useful for polling the status of a report generation.

    Args:
        job_id: ID of the report job

    Returns:
        Full job details including status
    """
    response: GenericResponseModel = ReportsService.get_report_status(job_id=job_id)
    return build_api_response(response)
