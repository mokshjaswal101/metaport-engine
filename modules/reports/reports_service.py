"""
Reports Service

Handles business logic for report generation and management.
"""

import http
from typing import Optional

from context_manager.context import context_user_data, get_db_session
from logger import logger

from models.report_job import ReportJob, ReportJobStatus
from schema.base import GenericResponseModel

from .reports_schema import (
    GenerateReportRequest,
    validate_report_type,
    get_report_name,
    get_available_reports,
)
from .celery_tasks import process_report_job
from modules.aws_s3.aws_s3 import generate_presigned_url


class ReportsService:
    """Service class for managing report generation."""

    @staticmethod
    def generate_report(
        request: GenerateReportRequest,
    ) -> GenericResponseModel:
        """
        Queue a new report generation job.

        Creates a ReportJob record and dispatches a Celery task
        to process it in the background.

        Args:
            request: Report generation request with type, format, and filters

        Returns:
            GenericResponseModel with job_id on success
        """
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id
            user_id = getattr(user_data, "user_id", None)
            user_email = getattr(user_data, "email", None)

            db = get_db_session()

            # Validate report type
            if not validate_report_type(request.report_type):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Invalid report type: {request.report_type}",
                )

            # Get report name
            report_name = get_report_name(request.report_type)

            # Create the job record
            job = ReportJob(
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                report_type=request.report_type,
                report_name=report_name,
                report_format=request.report_format,
                filters=request.filters,
                status=ReportJobStatus.PENDING.value,
            )

            db.add(job)
            db.flush()  # Get the ID

            logger.info(
                extra=context_user_data.get(),
                msg=f"Created report job: {job.id}, type: {request.report_type}",
            )

            # Dispatch Celery task
            try:
                task = process_report_job.delay(job.id)
                job.celery_task_id = task.id
                db.flush()
            except Exception as e:
                # If Celery dispatch fails, still return success
                # The job can be picked up by the worker later
                logger.warning(
                    extra=context_user_data.get(),
                    msg=f"Failed to dispatch Celery task for job {job.id}: {str(e)}",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.ACCEPTED,
                status=True,
                message="Report generation queued successfully. Check the Downloads section for progress.",
                data={
                    "job_id": job.id,
                    "report_type": request.report_type,
                    "report_name": report_name,
                },
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error creating report job: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to queue report generation. Please try again.",
            )

    @staticmethod
    def get_downloads(
        page: int = 1,
        page_size: int = 10,
        status: Optional[str] = None,
    ) -> GenericResponseModel:
        """
        Get paginated list of report downloads for the current client.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Optional status filter

        Returns:
            GenericResponseModel with downloads list and pagination
        """
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # Validate status filter if provided
            if status and status not in [s.value for s in ReportJobStatus]:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Invalid status filter: {status}",
                )

            # Get paginated downloads
            jobs, total_count = ReportJob.get_downloads_for_client(
                db_session=db,
                client_id=client_id,
                page=page,
                page_size=page_size,
                status=status,
            )

            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size

            # Build response
            downloads = [job.to_list_dict() for job in jobs]

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "downloads": downloads,
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": total_pages,
                        "has_next": page < total_pages,
                        "has_prev": page > 1,
                    },
                },
                message="Downloads retrieved successfully",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching downloads: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch downloads. Please try again.",
            )

    @staticmethod
    def get_download_url(job_id: int) -> GenericResponseModel:
        """
        Get a presigned download URL for a completed report.

        Args:
            job_id: ID of the report job

        Returns:
            GenericResponseModel with presigned URL
        """
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # Get the job (ensuring it belongs to this client)
            job = ReportJob.get_by_id_and_client(
                db_session=db,
                job_id=job_id,
                client_id=client_id,
            )

            if not job:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Report not found",
                )

            if job.status != ReportJobStatus.COMPLETED.value:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Report is not ready for download. Status: {job.status}",
                )

            if not job.s3_key:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Report file not found",
                )

            # Generate presigned URL (15 minutes expiry)
            url_result = generate_presigned_url(
                s3_key=job.s3_key,
                expires_in=900,
                file_name=job.file_name,
            )

            if not url_result.get("success"):
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Failed to generate presigned URL for job {job_id}: {url_result.get('error')}",
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Failed to generate download URL",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "download_url": url_result["url"],
                    "file_name": job.file_name,
                    "expires_in_seconds": url_result["expires_in"],
                },
                message="Download URL generated successfully",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error generating download URL: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to generate download URL. Please try again.",
            )

    @staticmethod
    def retry_report(job_id: int) -> GenericResponseModel:
        """
        Retry a failed report generation.

        Args:
            job_id: ID of the failed report job

        Returns:
            GenericResponseModel with retry status
        """
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # Get the job
            job = ReportJob.get_by_id_and_client(
                db_session=db,
                job_id=job_id,
                client_id=client_id,
            )

            if not job:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Report not found",
                )

            if not job.can_retry():
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Report cannot be retried. Status: {job.status}, Retries: {job.retry_count}/{job.max_retries}",
                )

            # Reset for retry
            job.increment_retry()
            db.flush()

            logger.info(
                extra=context_user_data.get(),
                msg=f"Retrying report job: {job_id}, attempt: {job.retry_count}",
            )

            # Dispatch Celery task
            try:
                task = process_report_job.delay(job.id)
                job.celery_task_id = task.id
                db.flush()
            except Exception as e:
                logger.warning(
                    extra=context_user_data.get(),
                    msg=f"Failed to dispatch Celery task for retry: {str(e)}",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Report retry queued successfully",
                data={
                    "job_id": job.id,
                    "retry_count": job.retry_count,
                },
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error retrying report: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to retry report. Please try again.",
            )

    @staticmethod
    def cancel_report(job_id: int) -> GenericResponseModel:
        """
        Cancel a pending report generation.

        Args:
            job_id: ID of the report job to cancel

        Returns:
            GenericResponseModel with cancellation status
        """
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # Get the job
            job = ReportJob.get_by_id_and_client(
                db_session=db,
                job_id=job_id,
                client_id=client_id,
            )

            if not job:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Report not found",
                )

            if job.status != ReportJobStatus.PENDING.value:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Only pending reports can be cancelled. Current status: {job.status}",
                )

            # Mark as cancelled
            job.mark_cancelled()
            db.flush()

            logger.info(
                extra=context_user_data.get(),
                msg=f"Cancelled report job: {job_id}",
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Report cancelled successfully",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error cancelling report: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to cancel report. Please try again.",
            )

    @staticmethod
    def get_report_types() -> GenericResponseModel:
        """
        Get list of available report types.

        Returns:
            GenericResponseModel with available report types
        """
        try:
            reports = get_available_reports()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "reports": [r.model_dump() for r in reports],
                },
                message="Report types retrieved successfully",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching report types: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch report types.",
            )

    @staticmethod
    def get_report_status(job_id: int) -> GenericResponseModel:
        """
        Get the status of a specific report job.

        Args:
            job_id: ID of the report job

        Returns:
            GenericResponseModel with job details
        """
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # Get the job
            job = ReportJob.get_by_id_and_client(
                db_session=db,
                job_id=job_id,
                client_id=client_id,
            )

            if not job:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Report not found",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=job.to_dict(),
                message="Report status retrieved successfully",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching report status: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch report status.",
            )
