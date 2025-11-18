import http
from sqlalchemy import or_, desc, cast, func, case
import pandas as pd
import pytz
from typing import List, Any, List, Dict
from fastapi import Response, FastAPI, File, UploadFile, Form
from psycopg2 import DatabaseError
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import joinedload
from sqlalchemy.types import DateTime, String
from typing import List
from io import BytesIO
from fastapi.encoders import jsonable_encoder
import json
from pydantic import BaseModel
from fastapi import APIRouter, Request, Depends, HTTPException
import hmac
from hashlib import sha256
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode, urlunparse, parse_qs, urlparse
from sqlalchemy.orm import load_only, joinedload, contains_eager, subqueryload
from sqlalchemy.dialects import postgresql
import os
import shutil
import time
import base64
from decimal import Decimal
import boto3
import uuid


from context_manager.context import context_user_data, get_db_session

from logger import logger
import requests


# schema
from schema.base import GenericResponseModel
from modules.discrepancie.discrepancie_schema import (
    upload_rate_discrepancie_model,
    Rate_Discrepancy_Response_Client_model,
    Report_Discrepancy_Uploaded_Response_model,
    Status_Model_Schema,
    # Rate_Discrepancy_Response_model_New,
    Accept_Description_Model,
    Accept_Bulk_Description_Model,
    Dispute_Model,
    Bulk_Dispute_Model,
    Status_Model,
    view_History_Schema,
    History_Schema_Response,
)

# service
from modules.serviceability import ServiceabilityService

# models
from models import (
    Admin_Rate_Discrepancie,
    Admin_Rate_Discrepancie_History,
    Admin_Rate_Discrepancie_Dispute,
    Order,
    # Aggregator_Courier,
    Aggregator_Courier,
    Company_To_Client_Contract,
    # COD_Remittance,
    Client,
)

# Status List
Discrepancie_status = {
    "New Discrepancies": "new_disc",
    "Re Schedule Discrepancies": "re_new_disc",
    "Discrepancies Auto-Accepted": "auto_accepted_Inactivity",
    "All Discrepancies": "all_disc",
    "Product Level Weight Intelligence": "level_weight_disc",
    "Accepted By Client": "accepted_Disc",
    "Accepted By Courier": "accepted_by_courier",
    "Dispute raise by client": "dispute_client",
    "Auto Accepted Due to Inactivity": "auto_accepted_Inactivity",
}


# AWS credentials


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")  #
S3_BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION_NAME = os.environ.get("REGION_NAME")

# Initialize the S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
)


Discrepancie_status_reverse = {v: k for k, v in Discrepancie_status.items()}

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)  # Ensure upload directory exists

seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)


def convert_decimal_to_float(value):
    """Recursively convert Decimal to float in dicts, lists, or standalone values."""
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, dict):
        return {k: convert_decimal_to_float(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_decimal_to_float(v) for v in value]
    return value


class DiscrepancieService:

    @staticmethod
    def create_history_record(db, record, status, action_by):
        history_record = Admin_Rate_Discrepancie_History(
            awb_number=record.awb_number,
            discrepancie_id=record.id,  # Store Admin_Rate_Discrepancie ID instead of Order ID
            length=convert_decimal_to_float(record.length),
            width=convert_decimal_to_float(record.width),
            height=convert_decimal_to_float(record.height),
            volumetric_weight=convert_decimal_to_float(record.volumetric_weight),
            dead_weight=convert_decimal_to_float(record.dead_weight),
            applied_weight=convert_decimal_to_float(record.applied_weight),
            courier_weight=convert_decimal_to_float(record.courier_weight),
            charged_weight=convert_decimal_to_float(record.charged_weight),
            charged_weight_charge=convert_decimal_to_float(
                record.charged_weight_charge
            ),
            excess_weight_charge=convert_decimal_to_float(record.excess_weight_charge),
            image1=record.image1,
            image2=record.image2,
            image3=record.image3,
            status=status,
            action_by=action_by,
        )

        db.add(history_record)
        return history_record  # Returning in case you need to use it later

    @staticmethod
    def bulk_create_history(db, records, status, action_by):
        print("**bulk_create_history**")
        history_objects = []
        for record in records:
            history_objects.append(
                Admin_Rate_Discrepancie_History(
                    awb_number=record.awb_number,
                    discrepancie_id=record.id,
                    length=convert_decimal_to_float(record.length),
                    width=convert_decimal_to_float(record.width),
                    height=convert_decimal_to_float(record.height),
                    volumetric_weight=convert_decimal_to_float(
                        record.volumetric_weight
                    ),
                    dead_weight=convert_decimal_to_float(record.dead_weight),
                    applied_weight=convert_decimal_to_float(record.applied_weight),
                    courier_weight=convert_decimal_to_float(record.courier_weight),
                    charged_weight=convert_decimal_to_float(record.charged_weight),
                    charged_weight_charge=convert_decimal_to_float(
                        record.charged_weight_charge
                    ),
                    excess_weight_charge=convert_decimal_to_float(
                        record.excess_weight_charge
                    ),
                    image1=record.image1,
                    image2=record.image2,
                    image3=record.image3,
                    status=status,
                    action_by=action_by,
                )
            )

        db.bulk_save_objects(history_objects)
        db.commit()

    @staticmethod
    def get_Client_name_by_id(id):
        res = Client.get_by_id(id)  # Call class method
        return res.client_name

    @staticmethod
    def all_ratelist(tab_action: Status_Model):
        try:
            client_id = context_user_data.get().client_id
            start_time = time.time()

            with get_db_session() as db:
                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD all_ratelist Client Side: {}".format(str(tab_action)),
                )

                page_number = tab_action.selectedPageNumber
                batch_size = tab_action.batchSize

                # ----------------------------
                # 1️ Bulk auto-accept old records
                # ----------------------------
                if Discrepancie_status[tab_action.status] == "new_disc":
                    old_ids = (
                        db.query(Admin_Rate_Discrepancie.id)
                        .filter(
                            Admin_Rate_Discrepancie.client_id == client_id,
                            Admin_Rate_Discrepancie.updated_at < seven_days_ago,
                            Admin_Rate_Discrepancie.status.in_(
                                ["new_disc", "re_new_disc"]
                            ),
                        )
                        .all()
                    )
                    old_ids = [r.id for r in old_ids]

                    if old_ids:
                        # Bulk update without ORM sync
                        db.query(Admin_Rate_Discrepancie).filter(
                            Admin_Rate_Discrepancie.id.in_(old_ids)
                        ).update(
                            {
                                "status": "auto_accepted_Inactivity",
                                "action_by": "warehousity",
                            },
                            synchronize_session=False,
                        )
                        db.commit()

                        # History bulk insert
                        DiscrepancieService.bulk_create_history(
                            db,
                            old_ids,
                            status="Auto Accepted Due to Inactivity",
                            action_by="warehousity",
                        )

                # ----------------------------
                # 2️ Build query for paginated listing
                # ----------------------------
                query = db.query(Admin_Rate_Discrepancie).filter(
                    Admin_Rate_Discrepancie.client_id == client_id
                )

                status_val = Discrepancie_status[tab_action.status]
                if status_val != "all_disc":
                    if status_val == "new_disc":
                        query = query.filter(
                            Admin_Rate_Discrepancie.status.in_(
                                ["new_disc", "re_new_disc"]
                            )
                        )
                    elif status_val == "dispute_client":
                        query = query.options(
                            joinedload(Admin_Rate_Discrepancie.disputes)
                        )
                        query = query.filter(
                            Admin_Rate_Discrepancie.status == status_val
                        )
                    else:
                        query = query.filter(
                            Admin_Rate_Discrepancie.status == status_val
                        )
                else:
                    # Exclude "new_disc" for history/all records
                    query = query.filter(Admin_Rate_Discrepancie.status != "new_disc")

                # Always eager load order (but avoid deep recursive loading)
                query = query.options(joinedload(Admin_Rate_Discrepancie.order))

                # ----------------------------
                # 3️ Optimize count query
                # ----------------------------
                total_count = db.query(func.count(Admin_Rate_Discrepancie.id)).filter(
                    Admin_Rate_Discrepancie.client_id == client_id
                )
                if status_val != "all_disc":
                    if status_val == "new_disc":
                        total_count = total_count.filter(
                            Admin_Rate_Discrepancie.status.in_(
                                ["new_disc", "re_new_disc"]
                            )
                        )
                    else:
                        total_count = total_count.filter(
                            Admin_Rate_Discrepancie.status == status_val
                        )
                else:
                    total_count = total_count.filter(
                        Admin_Rate_Discrepancie.status != "new_disc"
                    )
                total_count = total_count.scalar()  # much faster than .count()

                # ----------------------------
                # 4️ Apply pagination
                # ----------------------------
                offset_value = (page_number - 1) * batch_size
                results = query.offset(offset_value).limit(batch_size).all()
                # print(jsonable_encoder(results))
                # ----------------------------
                # 5️ Format response
                # ----------------------------
                data = [
                    Rate_Discrepancy_Response_Client_model(
                        **{
                            **jsonable_encoder(item),
                            "status": Discrepancie_status_reverse.get(
                                item.status, item.status
                            ),
                            "client_name": DiscrepancieService.get_Client_name_by_id(
                                item.client_id
                            ),
                        }
                    )
                    for item in results
                ]

                execution_time = time.time() - start_time
                print(f"Query executed in {execution_time:.4f} seconds")

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"discrepancy": data, "total_count": total_count},
                    message="Get rate list",
                )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Error fetching Order: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the Orders.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def view_history(view_history: view_History_Schema):
        try:
            with get_db_session() as db:

                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD get rate list all: {}".format(str("")),
                )
                history = (
                    db.query(Admin_Rate_Discrepancie_History)
                    .filter(
                        Admin_Rate_Discrepancie_History.discrepancie_id
                        == view_history.id
                    )
                    .all()
                )

                # Convert ORM objects to dicts compatible with Pydantic
                history_data = [
                    {
                        "awb_number": record.awb_number,
                        "action_by": record.action_by,
                        "status": record.status,
                        "created_at": record.created_at,
                    }
                    for record in history
                ]
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=History_Schema_Response(history=history_data),
                    message="View History",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetchin the Orders.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def accept_Description(accept_description: Accept_Description_Model):
        try:
            with get_db_session() as db:
                existing_record = (
                    db.query(Admin_Rate_Discrepancie)
                    .filter_by(awb_number=accept_description.awb_number)
                    .first()
                )
                if existing_record:
                    print(jsonable_encoder(existing_record))
                    existing_record.status = "accepted_Disc"
                    existing_record.action_by = "Client"
                    db.add(existing_record)
                    DiscrepancieService.create_history_record(
                        db,
                        existing_record,  # Use returned object
                        status="Accepted By Client",
                        action_by="client",
                    )

                    db.commit()
                # return serialized_results

                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD get rate list all: {}".format(str("")),
                )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="update record",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetchin the Orders.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def accept_bulk_Description(
        accept_bulk_description: Accept_Bulk_Description_Model,
    ):
        try:
            with get_db_session() as db:
                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD accept_bulk_Description: {}".format(
                        str(accept_bulk_description)
                    ),
                )

                for awb in accept_bulk_description.awb_numbers:
                    existing_record = (
                        db.query(Admin_Rate_Discrepancie)
                        .filter_by(awb_number=awb)
                        .first()
                    )
                    if existing_record:
                        existing_record.status = "accepted_Disc"
                        existing_record.action_by = "Client"
                        db.add(existing_record)
                        DiscrepancieService.create_history_record(
                            db,
                            existing_record,  # Use returned object
                            status="Accepted By Client",
                            action_by="client",
                        )

                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="update record",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetchin the Orders.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def dispute_file_upload(file: UploadFile = File(...), category: str = Form(...)):
        try:
            with get_db_session() as db:
                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD get dispute_file_upload: {}".format(str(file)),
                )
                client_id = context_user_data.get().client_id

                file_extension = file.filename.split(".")[
                    -1
                ].lower()  # Get file extension
                allowed_extensions = {"jpg", "jpeg", "png"}
                # Validate file type
                if file_extension not in allowed_extensions:
                    raise ValueError("Invalid file type. Allowed: jpg, jpeg, png")
                unique_filename = f"{uuid.uuid4()}.{file_extension}"
                print("s3 . 1")
                unique_filename = f"{uuid.uuid4()}.{file_extension}"
                s3_key = f"{client_id}/Weight_Discrepancie/{unique_filename}"
                print("weight descriupancy is available")
                s3_client.upload_fileobj(
                    file.file,
                    S3_BUCKET_NAME,
                    s3_key,
                    ExtraArgs={"ContentType": file.content_type},
                )
                print("s3 . 2")
                # file_url = f"https://{S3_BUCKET_NAME}.s3.{REGION_NAME}.amazonaws.com/{unique_filename}"
                file_url = (
                    f"https://{S3_BUCKET_NAME}.s3.{REGION_NAME}.amazonaws.com/{s3_key}"
                )
                print("s3 . 3")
                # Generate a new filename with timestamp
                timestamp = int(time.time())  # Get current timestamp
                new_filename = f"{category}_{timestamp}.{file_extension}"

                # file_location = os.path.join(UPLOAD_DIR, new_filename)

                # with open(file_location, "wb") as buffer:
                #     shutil.copyfileobj(file.file, buffer)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data={"file": file_url, "name": category, "s3": file_url},
                    status=True,
                    message="File Uploaded Successfully",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing dispute: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the dispute.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def dispute(dispute: Dispute_Model):
        try:
            with get_db_session() as db:
                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD get dispute: {}".format(str(dispute)),
                )

                dispute = Admin_Rate_Discrepancie_Dispute(**dispute.dict())
                # Add and commit (assuming `db` is your SQLAlchemy session)
                db.add(dispute)
                db.commit()
                last_inserted_id = dispute.id
                # print(2)
                if last_inserted_id:
                    # print(3)
                    discrepancie = (
                        db.query(Admin_Rate_Discrepancie)
                        .filter_by(awb_number=dispute.awb_number)
                        .first()
                    )
                    # print(4)
                    # print(discrepancie, "**discrepancie**")
                    if discrepancie:
                        # print(5)
                        discrepancie.status = "dispute_client"
                        discrepancie.action_by = "Client"
                        db.add(discrepancie)
                        DiscrepancieService.create_history_record(
                            db,
                            discrepancie,  # Use returned object
                            status="Dispute",
                            action_by="client",
                        )
                        db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="dispute",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing dispute: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the dispute.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    # Bulk Dispute
    @staticmethod
    def bulk_dispute(bulk_dispute: Bulk_Dispute_Model):
        try:
            with get_db_session() as db:
                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD get dispute: {}".format(str(bulk_dispute)),
                )
                for awb in bulk_dispute.awb_numbers:
                    modified_data = bulk_dispute.dict()
                    modified_data["awb_number"] = awb
                    modified_data.pop("awb_numbers", None)
                    dispute = Admin_Rate_Discrepancie_Dispute(**modified_data)
                    db.add(dispute)
                    db.commit()
                    last_inserted_id = dispute.id
                    print(last_inserted_id, "||ast_inserted_id||")
                    if last_inserted_id:
                        discrepancie = (
                            db.query(Admin_Rate_Discrepancie)
                            .filter_by(awb_number=modified_data["awb_number"])
                            .first()
                        )
                        if discrepancie:
                            discrepancie.status = "dispute_client"
                            discrepancie.action_by = "Client"
                            db.add(discrepancie)
                            DiscrepancieService.create_history_record(
                                db,
                                discrepancie,  # Use returned object
                                status="Dispute",
                                action_by="client",
                            )

                    db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="dispute",
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing dispute: {}".format(str(e)),
            )
            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the dispute.",
            )
        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_filtered_data(data: dict, discrepancie_type) -> dict:
        key_mapping = {
            "forward_freight": "freight",
            "forward_cod_charge": "cod_charges",
            "rto_freight": "rto_freight",
            "rto_tax": "rto_tax",
            "forward_tax": "forward_tax",
        }
        if discrepancie_type == "rto":
            key_mapping = {
                "rto_freight": "rto_freight",
                "rto_tax": "rto_tax",
            }
        if discrepancie_type == "forward":
            key_mapping = {
                "forward_freight": "freight",
                "forward_cod_charge": "cod_charges",
                "forward_tax": "forward_tax",
            }
        return {
            key_mapping[k]: data.get(k) or 0  # Defaults None to 0
            for k in key_mapping
            if k in data
        }

    @staticmethod
    def calculate_applied_weight_sum(data, discrepancie_type):
        from fastapi.encoders import jsonable_encoder

        data_dict = jsonable_encoder(data)  # Convert model to dict safely
        filtered_data = DiscrepancieService.get_filtered_data(
            data_dict, discrepancie_type
        )
        return round(sum(float(v or 0) for v in filtered_data.values()), 2)

    @staticmethod
    def calculate_total_charges(charged_data: dict, discrepancie_type) -> float:
        if discrepancie_type == "rto":
            return round(
                +float(charged_data.get("rto", 0) or 0)
                + float(charged_data.get("rto_tax", 0) or 0),
                2,
            )
        if discrepancie_type == "forward":
            return round(
                +float(charged_data.get("freight_charges", 0) or 0)
                + float(charged_data.get("freight_gst", 0) or 0),
                2,
            )

        return round(
            float(charged_data.get("freight_charges", 0) or 0)
            + float(charged_data.get("freight_gst", 0) or 0)
            + float(charged_data.get("rto", 0) or 0)
            + float(charged_data.get("rto_tax", 0) or 0),
            2,
        )

    def calculate_total_excess(data: dict, discrepancie_type) -> float:
        keys_to_sum = ["freight", "cod_charges", "tax_amount", "rto_freight", "rto_tax"]
        if discrepancie_type == "rto":
            keys_to_sum = [
                "rto_freight",
                "rto_tax",
            ]
        if discrepancie_type == "forward":
            keys_to_sum = [
                "freight",
                "cod_charges",
                "tax_amount",
            ]

        return round(sum(float(data.get(k, 0) or 0) for k in keys_to_sum), 2)

    @staticmethod
    def generate_report(status: Status_Model_Schema):
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id
                logger.info(
                    extra=context_user_data.get(),
                    msg="PAYLOAD status: {}".format(status),
                )
                query = db.query(Admin_Rate_Discrepancie).options(
                    joinedload(Admin_Rate_Discrepancie.order),
                )
                if Discrepancie_status[status.status] == "all_disc":
                    query = query.filter(
                        Admin_Rate_Discrepancie.status != "new_disc",
                        Admin_Rate_Discrepancie.client_id == client_id,
                    )
                elif Discrepancie_status[status.status] == "new_disc":
                    query = query.filter(
                        Admin_Rate_Discrepancie.status.in_(["new_disc", "re_new_disc"]),
                        Admin_Rate_Discrepancie.client_id == client_id,
                    )
                else:
                    query = query.filter(
                        Admin_Rate_Discrepancie.status
                        == Discrepancie_status[status.status],
                        Admin_Rate_Discrepancie.client_id == client_id,
                    )
                results = query.all()
                if status.action == "uploaded":
                    report = [
                        Report_Discrepancy_Uploaded_Response_model(
                            **{
                                **jsonable_encoder(item),
                                "status": Discrepancie_status_reverse.get(
                                    item.status, item.status
                                ),
                                "client_name": DiscrepancieService.get_Client_name_by_id(
                                    item.client_id
                                ),
                            }
                        )
                        for item in results
                    ]
                else:
                    report = [
                        {
                            "AWB Number": item.awb_number,
                            "Client Name": DiscrepancieService.get_Client_name_by_id(
                                item.client_id
                            ),
                            "Initial Length": item.order.length if item.order else None,
                            "Initial Width": item.order.breadth if item.order else None,
                            "Initial Height": item.order.height if item.order else None,
                            "Initial Volumetric Weight": (
                                item.order.volumetric_weight if item.order else None
                            ),
                            "Initial Applicable Weight": (
                                item.order.applicable_weight if item.order else None
                            ),
                            "Initial Total Weight": item.applied_weight,
                            "Initial Total Freight": (
                                DiscrepancieService.calculate_applied_weight_sum(
                                    item.order, item.discrepancie_type.value
                                )
                                if item.order
                                else None
                            ),
                            "Charged Length": item.length,
                            "Charged Width": item.width,
                            "Charged Height": item.height,
                            "Charged Volumetric Weight": item.volumetric_weight,
                            "Charged Applicable Weight": item.dead_weight,
                            "Charged Total Weight": item.charged_weight,
                            "Charged Total Freight": (
                                DiscrepancieService.calculate_total_charges(
                                    item.charged_weight_charge.get("charged", 0),
                                    item.discrepancie_type.value,
                                )
                                if item.charged_weight_charge
                                else None
                            ),
                            "Excess Weight": (
                                float(item.charged_weight) - float(item.applied_weight)
                                if item.charged_weight and item.applied_weight
                                else None
                            ),
                            "Excess Freight": (
                                DiscrepancieService.calculate_total_excess(
                                    item.excess_weight_charge,
                                    item.discrepancie_type.value,
                                )
                                if item.excess_weight_charge
                                else None
                            ),
                            "Discrepancie Type": item.discrepancie_type.upper(),
                            "Image 1": item.image1,
                            "Image 2": item.image2,
                            "Image 3": item.image3,
                            "Status": Discrepancie_status_reverse.get(
                                item.status, item.status
                            ),
                        }
                        for item in results
                    ]
                df = pd.DataFrame([jsonable_encoder(item) for item in report])
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Weight_Discrepancie")
                    if status.action == "full":
                        workbook = writer.book
                        worksheet = writer.sheets["Weight_Discrepancie"]
                        highlight_format = workbook.add_format({"bg_color": "#EEECE1"})
                        worksheet.set_column("I:I", 22, highlight_format)
                        worksheet.set_column("P:P", 22, highlight_format)
                        worksheet.set_column("R:R", 22, highlight_format)
                output.seek(0)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=base64.b64encode(output.getvalue()).decode("utf-8"),
                    message="Report gernated",
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing dispute: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the dispute.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )
