import boto3
import io
import os
from datetime import datetime
from botocore.exceptions import BotoCoreError, ClientError
from typing import IO, Optional

from logger import logger


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
REGION_NAME = os.environ.get("REGION_NAME")

# Initialize the S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
)


def upload_file_to_s3(
    file_obj: IO,
    s3_key: str,
    content_type: str = "application/octet-stream",
) -> dict:
    """
    Upload a file object to S3.

    Args:
        file_obj: File-like object to upload
        s3_key: The S3 key (path) where the file will be stored
        content_type: MIME type of the file

    Returns:
        dict with success status and URL or error
    """
    try:
        s3_client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={"ContentType": content_type},
        )

        file_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_key}"

        return {"success": True, "url": file_url, "s3_key": s3_key}

    except (BotoCoreError, ClientError) as e:
        logger.error(msg=f"S3 upload failed: {str(e)}")
        return {"success": False, "error": str(e)}


def upload_bytes_to_s3(
    content: bytes,
    s3_key: str,
    content_type: str = "application/octet-stream",
) -> dict:
    """
    Upload bytes content to S3.

    Args:
        content: Bytes content to upload
        s3_key: The S3 key (path) where the file will be stored
        content_type: MIME type of the file

    Returns:
        dict with success status, URL, s3_key, and file_size or error
    """
    try:
        file_obj = io.BytesIO(content)

        s3_client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={"ContentType": content_type},
        )

        file_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_key}"

        return {
            "success": True,
            "url": file_url,
            "s3_key": s3_key,
            "file_size": len(content),
        }

    except (BotoCoreError, ClientError) as e:
        logger.error(msg=f"S3 bytes upload failed: {str(e)}")
        return {"success": False, "error": str(e)}


def delete_file_from_s3(s3_key: str) -> dict:
    """
    Delete a file from S3

    Args:
        s3_key (str): The S3 key/path of the file to delete

    Returns:
        dict: Success status and any error message
    """
    try:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_key)

        return {"success": True}

    except (BotoCoreError, ClientError) as e:
        logger.error(msg=f"S3 delete failed: {str(e)}")
        return {"success": False, "error": str(e)}


def generate_presigned_url(
    s3_key: str,
    expires_in: int = 900,  # 15 minutes default
    file_name: Optional[str] = None,
) -> dict:
    """
    Generate a presigned URL for downloading a file from S3.

    Args:
        s3_key: The S3 key of the file
        expires_in: URL expiry time in seconds (default 15 minutes)
        file_name: Optional file name for Content-Disposition header

    Returns:
        dict with success status and presigned URL or error
    """
    try:
        params = {
            "Bucket": S3_BUCKET_NAME,
            "Key": s3_key,
        }

        # Add Content-Disposition to trigger download with specific filename
        if file_name:
            params["ResponseContentDisposition"] = f'attachment; filename="{file_name}"'

        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=expires_in,
        )

        return {
            "success": True,
            "url": presigned_url,
            "expires_in": expires_in,
        }

    except (BotoCoreError, ClientError) as e:
        logger.error(msg=f"Presigned URL generation failed: {str(e)}")
        return {"success": False, "error": str(e)}


def build_report_s3_key(
    client_id: int,
    report_type: str,
    file_name: str,
) -> str:
    """
    Build the S3 key for a report file following the folder structure:
    clients/{client_id}/reports/{report_type}/{year}/{month}/{file_name}

    Args:
        client_id: Client ID
        report_type: Type of report (e.g., 'pickup_locations')
        file_name: Name of the file

    Returns:
        S3 key string
    """
    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")

    return f"clients/{client_id}/reports/{report_type}/{year}/{month}/{file_name}"


def get_content_type_for_format(report_format: str) -> str:
    """
    Get the MIME content type for a report format.

    Args:
        report_format: Report format (csv, xlsx, pdf)

    Returns:
        MIME content type string
    """
    content_types = {
        "csv": "text/csv",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pdf": "application/pdf",
    }
    return content_types.get(report_format, "application/octet-stream")
