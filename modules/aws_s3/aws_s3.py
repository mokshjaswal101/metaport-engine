import boto3
import os
from botocore.exceptions import BotoCoreError, ClientError
from typing import IO


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")  #
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

    try:
        s3_client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={"ContentType": content_type},
        )

        file_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_key}"

        return {"success": True, "url": file_url}

    except (BotoCoreError, ClientError) as e:
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
        return {"success": False, "error": str(e)}
