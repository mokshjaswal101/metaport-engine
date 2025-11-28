"""
Celery Tasks for Report Generation

This module contains the Celery tasks for background report processing.
Tasks are dispatched by the reports service and executed by Celery workers.
"""

from celery import shared_task
from celery.exceptions import MaxRetriesExceededError
from sqlalchemy.orm import Session

from celery_app import celery_app
from database.db import SessionLocal
from logger import logger

from models.report_job import ReportJob, ReportJobStatus
from modules.reports.generators import get_generator
from modules.aws_s3.aws_s3 import (
    upload_bytes_to_s3,
    build_report_s3_key,
    get_content_type_for_format,
)


@celery_app.task(
    bind=True,
    name="modules.reports.celery_tasks.process_report_job",
    max_retries=3,
    default_retry_delay=60,  # 1 minute
    autoretry_for=(Exception,),
    retry_backoff=True,  # Exponential backoff
    retry_backoff_max=600,  # Max 10 minutes between retries
    acks_late=True,
)
def process_report_job(self, job_id: int):
    """
    Process a report generation job.

    This task:
    1. Fetches the job from database
    2. Updates status to PROCESSING
    3. Runs the appropriate generator
    4. Uploads the result to S3
    5. Updates job with file URL and COMPLETED status

    On failure, the job is marked as FAILED with error details.

    Args:
        job_id: ID of the ReportJob to process
    """
    db: Session = None
    job: ReportJob = None

    try:
        # Create a new database session for this task
        db = SessionLocal()

        # Fetch the job
        job = db.query(ReportJob).filter(ReportJob.id == job_id).first()

        if not job:
            logger.error(msg=f"Report job not found: {job_id}")
            return {"success": False, "error": "Job not found"}

        if job.status not in [
            ReportJobStatus.PENDING.value,
            ReportJobStatus.PROCESSING.value,
        ]:
            logger.info(msg=f"Job {job_id} is not in a processable state: {job.status}")
            return {"success": False, "error": f"Job not processable: {job.status}"}

        logger.info(msg=f"Processing report job: {job_id}, type: {job.report_type}")

        # Mark as processing
        job.mark_processing()
        job.celery_task_id = self.request.id
        db.commit()

        # Get the generator for this report type
        generator_class = get_generator(job.report_type)

        if not generator_class:
            raise ValueError(f"Unknown report type: {job.report_type}")

        # Initialize and run the generator
        generator = generator_class(
            db_session=db,
            client_id=job.client_id,
            company_id=job.company_id,
            filters=job.filters,
        )

        result = generator.generate()

        if not result.success:
            raise Exception(result.error_message or "Report generation failed")

        # Upload to S3
        s3_key = build_report_s3_key(
            client_id=job.client_id,
            report_type=job.report_type,
            file_name=result.file_name,
        )

        content_type = get_content_type_for_format(job.report_format)

        upload_result = upload_bytes_to_s3(
            content=result.content,
            s3_key=s3_key,
            content_type=content_type,
        )

        if not upload_result.get("success"):
            raise Exception(f"S3 upload failed: {upload_result.get('error')}")

        # Mark job as completed
        job.mark_completed(
            file_url=upload_result["url"],
            file_name=result.file_name,
            s3_key=s3_key,
            records_count=result.records_count,
            file_size_bytes=upload_result.get("file_size"),
        )
        db.commit()

        logger.info(
            msg=f"Report job completed: {job_id}, records: {result.records_count}"
        )

        return {
            "success": True,
            "job_id": job_id,
            "records_count": result.records_count,
            "file_name": result.file_name,
        }

    except MaxRetriesExceededError:
        # All retries exhausted
        if job and db:
            job.mark_failed(
                error_message="Max retries exceeded", error_code="MAX_RETRIES_EXCEEDED"
            )
            db.commit()
        logger.error(msg=f"Report job {job_id} failed after max retries")
        raise

    except Exception as e:
        error_message = str(e)
        logger.error(msg=f"Report job {job_id} failed: {error_message}")

        # Check if we should retry
        if self.request.retries < self.max_retries:
            # Will retry - don't mark as failed yet
            if job and db:
                job.retry_count = self.request.retries + 1
                db.commit()
            raise  # Let Celery handle the retry
        else:
            # Final failure - mark job as failed
            if job and db:
                job.mark_failed(
                    error_message=error_message, error_code="GENERATION_FAILED"
                )
                db.commit()
            raise

    finally:
        if db:
            db.close()


@celery_app.task(
    name="modules.reports.celery_tasks.cleanup_expired_reports",
)
def cleanup_expired_reports():
    """
    Cleanup task to delete expired report files from S3.

    This task should be scheduled to run periodically (e.g., daily).
    It finds all completed reports that have expired and:
    1. Deletes the file from S3
    2. Soft-deletes the job record

    Note: This is a maintenance task, not called during normal operation.
    """
    from modules.aws_s3.aws_s3 import delete_file_from_s3

    db: Session = None

    try:
        db = SessionLocal()

        # Get expired jobs
        expired_jobs = ReportJob.get_expired_jobs(db, batch_size=100)

        deleted_count = 0

        for job in expired_jobs:
            try:
                # Delete from S3
                if job.s3_key:
                    delete_result = delete_file_from_s3(job.s3_key)
                    if not delete_result.get("success"):
                        logger.warning(
                            msg=f"Failed to delete S3 file for job {job.id}: {delete_result.get('error')}"
                        )

                # Soft delete the job record
                job.is_deleted = True
                job.file_url = None
                job.s3_key = None
                deleted_count += 1

            except Exception as e:
                logger.error(msg=f"Error cleaning up job {job.id}: {str(e)}")
                continue

        db.commit()

        logger.info(msg=f"Cleaned up {deleted_count} expired report jobs")

        return {"success": True, "deleted_count": deleted_count}

    except Exception as e:
        logger.error(msg=f"Error in cleanup task: {str(e)}")
        return {"success": False, "error": str(e)}

    finally:
        if db:
            db.close()
