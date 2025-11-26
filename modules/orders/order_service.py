import http
from uuid import uuid4
from dateutil import parser as date_parser
import base64

from sqlalchemy import (
    or_,
    desc,
    cast,
    func,
    text,
    and_,
)
from sqlalchemy.dialects.postgresql import JSON
from fastapi import (
    # APIRouter,
    File,
    UploadFile,
)
import pandas as pd
from fastapi import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from psycopg2 import DatabaseError
from datetime import datetime, timedelta, timezone, date
from sqlalchemy.orm import joinedload
from fastapi.encoders import jsonable_encoder
from sqlalchemy.types import DateTime, String
from typing import List
from io import BytesIO
import json
import time
import base64
import pytz
from pydantic import BaseModel
import re
import requests
import httpx
import uuid
import logging
from urllib.parse import quote

from context_manager.context import context_user_data, get_db_session
from utils.error_excel_generator import ErrorExcelGenerator

from logger import logger

# schema
from schema.base import GenericResponseModel
from .order_schema import (
    Order_create_request_model,
    Order_Response_Model,
    Single_Order_Response_Model,
    Order_filters,
    cloneOrderModel,
    customerResponseModel,
    BulkDimensionUpdateModel,
    UpdatePickupLocationModel,
    BulkImportValidationError,
    BulkImportResponseModel,
)
from modules.shipment.shipment_schema import CreateShipmentModel

from shipping_partner.shiperfecto.status_mapping import status_mapping

# models
from models import (
    Order,
    Pickup_Location,
    Pincode_Mapping,
    Company_To_Client_Contract,
    Aggregator_Courier,
    COD_Remittance,
    Company_Contract,
    Wallet,
    Wallet_Logs,
    cod_remmittance,
    Courier_Priority,
    Courier_Priority_Rules,
    Client,
    Courier_Priority_Config_Setting,
    Pincode_Serviceability,
    Ndr_history,
    Ndr,
    Company_To_Client_COD_Rates,
    Company_To_Client_Rates,
    New_Company_To_Client_Rate,
    BulkOrderUploadLogs,
)

# service
from modules.shipment import ShipmentService
from modules.serviceability import ServiceabilityService
from modules.wallet import WalletService
from modules.aws_s3.aws_s3 import upload_file_to_s3
from data.Locations import metro_cities, special_zone
import random
import os


original_tz = "Asia/Kolkata"


class TempModel(BaseModel):
    client_id: int


def round_to_2_decimal_place(value):
    """Round a float to 2 decimal places."""
    return round(value, 2)


def calculate_order_values(order_data):
    # Calculate order_value based on products
    order_value = sum(
        float(product.get("unit_price", 0.0)) * int(product.get("quantity", 0))
        for product in json.loads(order_data["products"])
    )

    # Calculate total_amount
    total_amount = (
        order_data.get("shipping_charges", 0)
        + order_data.get("cod_charges", 0)
        + order_data.get("gift_wrap_charges", 0)
        + order_data.get("other_charges", 0)
        + order_value
        - order_data.get("discount", 0)
    )

    # Round to 2 decimal places
    order_value = round_to_2_decimal_place(order_value)
    total_amount = round_to_2_decimal_place(total_amount)

    return order_value, total_amount


# Helper functions
def safe(value, default=None):
    return value if value not in (None, "") else default


def parse_numeric(value, default=0):
    """Convert value to float safely. Returns default if conversion fails."""
    try:
        if value in (None, "", "NA"):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def parse_datetime(value):
    """Convert string to datetime safely. Returns None if invalid."""
    try:
        if value in (None, "", "NA"):
            return None
        if isinstance(value, datetime):
            return value
        return date_parser.parse(value)
    except Exception:
        return None


def parse_datetime_safe(value):
    try:
        if value:
            # Try parsing string to datetime if needed
            return parse_datetime(value)  # your existing parser
        else:
            return datetime.now(timezone.utc)  # fallback to current UTC
    except Exception:
        return datetime.now(timezone.utc)


def get_next_wednesday(date):
    # Add D+5 (5 days after delivery)
    d_plus_5 = date + timedelta(days=5)

    # Get the next Wednesday after D+5
    days_ahead = (2 - d_plus_5.weekday() + 7) % 7  # Wednesday is weekday 2
    return d_plus_5 + timedelta(days=days_ahead)


def update_or_create_cod_remittance(order, db):
    delivered_status = "delivered"

    # Fetch the delivered date from tracking_info JSON field
    for status_info in order.tracking_info:
        if status_info["status"] == delivered_status:
            delivered_date = datetime.strptime(
                status_info["datetime"], "%d-%m-%Y %H:%M:%S"
            ).date()

    # Calculate the next Wednesday after D+5
    remittance_date = get_next_wednesday(delivered_date)

    # Check if there is already a COD remittance entry for the calculated remittance_date
    cod_remittance = (
        db.query(COD_Remittance)
        .filter(
            COD_Remittance.payout_date == remittance_date,
            COD_Remittance.client_id == order.client_id,
        )
        .first()
    )

    if cod_remittance:
        # Update existing remittance
        cod_remittance.generated_cod += order.total_amount
        cod_remittance.order_count += 1
    else:
        # Create a new COD remittance entry
        cod_remittance = COD_Remittance(
            payout_date=remittance_date,
            generated_cod=order.total_amount,
            order_count=1,
            client_id=order.client_id,
            status="pending",
        )
        db.add(cod_remittance)

    # Commit the remittance entry
    db.flush()

    print(cod_remittance.id)

    return cod_remittance.id


def convert_to_utc(order_date, original_tz="Asia/Kolkata"):
    # Check if order_date is a string and needs parsing
    if isinstance(order_date, str):
        # Parse the date from the string if it's a string
        order_date = datetime.strptime(order_date, "%Y-%m-%d %H:%M:%S")

    # Set the original timezone if it's naive
    local_tz = pytz.timezone(original_tz)
    if order_date.tzinfo is None:
        order_date_localized = local_tz.localize(order_date)
    else:
        order_date_localized = order_date

    # Convert to UTC
    order_date_utc = order_date_localized.astimezone(pytz.utc)

    print(order_date_utc)

    return order_date_utc


"""
Order Service Module - Optimized for High-Performance Bulk Operations

PERFORMANCE OPTIMIZATION NOTES:
================================

For optimal bulk import performance, ensure the following database indexes exist:

1. Composite index on orders table for duplicate checking:
   CREATE INDEX CONCURRENTLY idx_order_lookup ON "order" (order_id, company_id, client_id);

2. Index on pickup_location for faster lookups:
   CREATE INDEX CONCURRENTLY idx_pickup_location_client ON pickup_location (client_id, location_code);

3. Partial index for active orders:
   CREATE INDEX CONCURRENTLY idx_order_active ON "order" (company_id, client_id) WHERE is_deleted = false;

4. Consider partitioning the orders table by company_id for very large datasets.

Database Settings for Bulk Operations:
- Increase shared_buffers to 25% of RAM
- Set work_mem to 256MB for bulk operations
- Temporarily disable synchronous_commit for bulk imports
- Use connection pooling to manage concurrent connections

Memory Settings:
- Ensure the application has sufficient memory for processing large batches
- Monitor memory usage during bulk operations to prevent OOM errors
"""


class OrderService:

    @staticmethod
    def _parse_validation_error(error_message: str) -> str:
        """
        Parse Pydantic validation error and extract essential information

        Example input: "Data validation failed: 1 validation error for Order_create_request_model
        billing_pincode
          Input should be a valid string [type=string_type, input_value=110075, input_type=int]"

        Output: "billing_pincode: Input should be a valid string"
        """
        try:
            lines = error_message.split("\n")
            field_name = None
            error_desc = None

            for i, line in enumerate(lines):
                line = line.strip()

                # Skip header lines and empty lines
                if (
                    not line
                    or "validation error" in line
                    or "Order_create_request_model" in line
                ):
                    continue

                # Field name is usually a single word line after the header
                if field_name is None and not line.startswith(" ") and ":" not in line:
                    field_name = line
                    continue

                # Error description starts with spaces and contains the actual error
                if field_name and line.startswith(" ") and "Input should be" in line:
                    # Extract just the error message before the brackets
                    if "[" in line:
                        error_desc = line.split("[")[0].strip()
                    else:
                        error_desc = line.strip()
                    break

                # Alternative format: "field_name: error message"
                if ":" in line and "Input should be" in line:
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        field_name = parts[0].strip()
                        error_desc = parts[1].strip()
                        if "[" in error_desc:
                            error_desc = error_desc.split("[")[0].strip()
                        break

                # Handle other validation types like required fields
                if (
                    field_name
                    and line.startswith(" ")
                    and ("required" in line.lower() or "missing" in line.lower())
                ):
                    error_desc = line.strip()
                    break

            # Return formatted error or fallback
            if field_name and error_desc:
                return f"Field '{field_name}': {error_desc}"
            elif field_name:
                return (
                    f"Field '{field_name}': Validation error - please check the format"
                )
            else:
                # Fallback for unrecognized format
                return "Data validation error - please check field formats"

        except Exception:
            # If parsing fails, return a generic message
            return "Data validation error - please check field formats"

    @staticmethod
    async def create_order(order_data: Order_create_request_model):
        db: AsyncSession = None
        try:
            print(order_data, "before process")

            courier_id = order_data.courier
            del order_data.courier

            db = get_db_session()  # Should return AsyncSession
            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # Check if order exists
            stmt = select(Order).where(
                Order.order_id == order_data.order_id,
                Order.company_id == company_id,
                Order.client_id == client_id,
            )
            result = await db.execute(stmt)
            order = result.scalar_one_or_none()
            print(1)
            if order:
                if order.status not in ["new", "cancelled"]:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        status=False,
                        data={
                            "awb_number": order.awb_number or "",
                            "delivery_partner": order.courier_partner or "",
                        },
                        message="AWB already assigned",
                    )

                if courier_id is not None and order.status == "new":
                    shipmentResponse = await ShipmentService.assign_awb(
                        CreateShipmentModel(
                            order_id=order_data.order_id,
                            contract_id=courier_id,
                        )
                    )
                    return shipmentResponse

                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    data={"order_id": order_data.order_id},
                    message="Order Id already exists",
                )

            order_data_dict = order_data.model_dump()
            order_data_dict.update(
                {
                    "client_id": client_id,
                    "company_id": company_id,
                    "order_type": "B2C",
                }
            )
            print(2)
            # Volumetric and applicable weight
            volumetric_weight = round(
                (
                    order_data_dict["length"]
                    * order_data_dict["breadth"]
                    * order_data_dict["height"]
                )
                / 5000,
                3,
            )
            applicable_weight = round(
                max(order_data_dict["weight"], volumetric_weight), 3
            )
            order_data_dict.update(
                {
                    "applicable_weight": applicable_weight,
                    "volumetric_weight": volumetric_weight,
                    "status": "new",
                    "sub_status": "new",
                    "action_history": [
                        {
                            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                            "message": "Order Created on Platform",
                            "user_data": context_user_data.get().id,
                        }
                    ],
                    "order_date": convert_to_utc(
                        order_date=order_data_dict["order_date"]
                    ),
                    "product_quantity": sum(
                        p["quantity"] for p in order_data_dict["products"]
                    ),
                }
            )

            # Fetch pickup pincode
            stmt = select(Pickup_Location.pincode).where(
                Pickup_Location.location_code
                == order_data_dict["pickup_location_code"],
                Pickup_Location.client_id == client_id,
            )
            result = await db.execute(stmt)
            pickup_pincode_row = result.scalar_one_or_none()
            print(3)
            if pickup_pincode_row is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pickup Location",
                )
            pickup_pincode = pickup_pincode_row

            # blocked_pickup_pincodes = [
            #     "110018",
            #     "110031",
            #     "110041",
            #     "110033",
            #     "110043",
            #     "110018",
            # ]
            # if client_id == 93 and pickup_pincode in blocked_pickup_pincodes:
            #     return GenericResponseModel(
            #         status_code=http.HTTPStatus.BAD_REQUEST,
            #         message="Pickup location not serviceable",
            #     )
            print(4)
            # Calculate shipping zone
            zone_data = await ShipmentService.calculate_shipping_zone(
                pickup_pincode, order_data_dict["consignee_pincode"]
            )
            print(5)
            if not zone_data.status:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pincodes",
                    status=False,
                )
            order_data_dict["zone"] = zone_data.data["zone"]
            print(6)
            # Create order instance
            order_model_instance = Order.create_db_entity(order_data_dict)
            db.add(order_model_instance)
            await db.commit()
            print(7)
            await db.refresh(order_model_instance)

            # Assign AWB if courier is provided
            if courier_id is not None:
                shipmentResponse = await ShipmentService.assign_awb(
                    CreateShipmentModel(
                        order_id=order_data_dict["order_id"],
                        contract_id=courier_id,
                    )
                )
                return shipmentResponse

            # Handle courier priority logic here (use async queries if necessary)
            # ...
            print(8)
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Order created Successfully",
                data={"order_id": order_model_instance.order_id},
                status=True,
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(), msg=f"Error creating Order: {e}"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
            )

        except Exception as e:
            logger.error(extra=context_user_data.get(), msg=f"Unhandled error: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

        finally:
            if db:
                await db.close()

    @staticmethod
    def dev_create_order(
        order_data: Order_create_request_model,
    ):

        print("developement")
        try:
            print(order_data)
            courier_id = order_data.courier
            del order_data.courier

            with get_db_session() as db:

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_data.order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                    .first()
                )

                # Throw an error if an order id for that client already exists
                if order:

                    if order.status != "new" and order.status != "cancelled":

                        if client_id == 93:

                            return GenericResponseModel(
                                status_code=http.HTTPStatus.OK,
                                status=False,
                                data={
                                    "awb_number": order.awb_number or "",
                                    "delivery_partner": order.courier_partner or "",
                                },
                                message="AWB already assigned",
                            )

                        else:

                            return GenericResponseModel(
                                status_code=http.HTTPStatus.CONFLICT,
                                status=False,
                                data={
                                    "awb_number": order.awb_number or "",
                                    "delivery_partner": order.courier_partner or "",
                                },
                                message="AWB already assigned",
                            )

                    if courier_id is not None and order.status == "new":
                        shipmentResponse = ShipmentService.dev_assign_awb(
                            order=order,
                            courier_id=courier_id,
                        )

                        return shipmentResponse

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        data={"order_id": order_data.order_id},
                        message="Order Id already exists",
                    )

                order_data = order_data.model_dump()

                if not re.search(
                    r"\d", order_data["consignee_address"]
                ):  # Check if there's at least one digit
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Consignee address must contain a house number or street number",
                    )

                # Add company and client id to the order

                order_data["client_id"] = client_id
                order_data["company_id"] = company_id

                # adding the extra default details to the order

                order_data["order_type"] = "B2C"

                # rounde the volumetric weight to 3 decimal places
                volumetric_weight = round(
                    (
                        order_data["length"]
                        * order_data["breadth"]
                        * order_data["height"]
                    )
                    / 5000,
                    3,
                )

                applicable_weight = round(
                    max(order_data["weight"], volumetric_weight), 3
                )

                order_data["applicable_weight"] = applicable_weight
                order_data["volumetric_weight"] = volumetric_weight

                order_data["status"] = "new"
                order_data["sub_status"] = "new"

                order_data["action_history"] = [
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "message": "Order Created on Platform",
                        "user_data": context_user_data.get().id,
                    }
                ]

                # Convert to UTC
                order_data["order_date"] = convert_to_utc(
                    order_date=order_data["order_date"]
                )

                # calc product quantity
                order_data["product_quantity"] = sum(
                    product["quantity"] for product in order_data["products"]
                )

                pickup_location = (
                    db.query(Pickup_Location.pincode)
                    .filter(
                        Pickup_Location.location_code
                        == order_data["pickup_location_code"],
                        Pickup_Location.client_id == client_id,
                    )
                    .first()
                )

                if not pickup_location:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pickup Location",
                    )

                # fetch the pickup location pincode
                pickup_pincode = pickup_location.pincode

                if pickup_pincode is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pickup Location",
                    )

                # calculating shipping zone for the order
                zone_data = ShipmentService.calculate_shipping_zone(
                    pickup_pincode, order_data["consignee_pincode"]
                )

                # return error message if could not calculate zone
                if not zone_data.status:
                    GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pincodes",
                        status=False,
                    )

                zone = zone_data.data["zone"]
                order_data["zone"] = zone

                order_model_instance = Order.create_db_entity(order_data)

                if courier_id == "1056" or courier_id == 1056:

                    awb = (
                        f"SITP0000{uuid4().int % 1000000:06d}"
                        if order_data["payment_mode"].lower() == "prepaid"
                        else f"C0000{uuid4().int % 1000000:06d}"
                    )

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        status=True,
                        data={
                            "awb_number": awb or "",
                            "delivery_partner": "Ekart 2 Kg" or "",
                        },
                        message="AWB assigned",
                    )

                if courier_id == "4057" or courier_id == 4057:

                    prefix = random.choice([17, 18])
                    suffix = uuid4().int % 10**8
                    awb = f"SF{prefix}{suffix:08d}WAO"

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        status=True,
                        data={
                            "awb_number": awb or "",
                            "delivery_partner": "Shadowfax" or "",
                        },
                        message="AWB assigned",
                    )

                else:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        status=False,
                        message="Invalid Courier Id",
                    )

                if courier_id is not None:
                    shipmentResponse = ShipmentService.dev_assign_awb(
                        order=created_order,
                        courier_id=courier_id,
                    )

                    print("inside shipment response", shipmentResponse)

                    # db.commit()

                    return shipmentResponse

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order created Successfully",
                    data={"order_id": created_order.order_id},
                    status=True,
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
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

        finally:
            if db:
                db.close()

    @staticmethod
    def dev_cancel_awbs():

        time.sleep(3)
        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="cancelled successfully",
        )

    @staticmethod
    async def update_order(order_id: str, order_data: Order_create_request_model):
        try:
            async with get_db_session() as db:
                print("Welcome to update order service")

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # -------------------------------
                # FETCH EXISTING ORDER
                # -------------------------------
                stmt = select(Order).where(
                    and_(
                        Order.order_id == order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                )
                result = await db.execute(stmt)
                order = result.scalar_one_or_none()

                if order is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        data={"order_id": order_id},
                        message="Order does not exist",
                    )

                if order.status != "new":
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order cannot be updated",
                    )

                # -------------------------------
                # CHECK FOR DUPLICATE ORDER ID
                # -------------------------------
                if order_id != order_data.order_id:
                    stmt2 = select(Order).where(
                        and_(
                            Order.order_id == order_data.order_id,
                            Order.company_id == company_id,
                            Order.client_id == client_id,
                        )
                    )
                    result2 = await db.execute(stmt2)
                    check_existing = result2.scalar_one_or_none()

                    if check_existing:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            data={"order_id": order_data.order_id},
                            message="Order id already exists",
                        )

                # Convert pydantic model to dict
                order_data_dict = order_data.model_dump()

                # -------------------------------
                # UPDATE ALL FIELDS
                # -------------------------------
                for key, value in order_data_dict.items():
                    setattr(order, key, value)

                # WEIGHT CALCULATIONS
                volumetric_weight = round(
                    (
                        order_data_dict["length"]
                        * order_data_dict["breadth"]
                        * order_data_dict["height"]
                    )
                    / 5000,
                    3,
                )

                applicable_weight = round(
                    max(order_data_dict["weight"], volumetric_weight),
                    3,
                )

                order.applicable_weight = applicable_weight
                order.volumetric_weight = volumetric_weight

                # CONVERT ORDER DATE TO UTC
                order.order_date = convert_to_utc(order.order_date)

                # -------------------------------
                # ACTIVITY HISTORY LOG
                # -------------------------------
                if order.action_history is None:
                    order.action_history = []

                new_activity = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Order Updated on Platform",
                    "user_data": context_user_data.get().id,
                }
                order.action_history.append(new_activity)

                # -------------------------------
                # FETCH PICKUP PINCODE
                # -------------------------------
                stmt3 = select(Pickup_Location.pincode).where(
                    Pickup_Location.location_code
                    == order_data_dict["pickup_location_code"]
                )
                result3 = await db.execute(stmt3)
                pickup_pincode_row = result3.first()

                if pickup_pincode_row is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pickup Location",
                    )

                pickup_pincode = int(pickup_pincode_row[0])

                # -------------------------------
                # SHIPPING ZONE CALCULATION
                # -------------------------------
                zone_data = await ShipmentService.calculate_shipping_zone(
                    pickup_pincode, order_data_dict["consignee_pincode"]
                )

                if not zone_data.status:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pincodes",
                        status=False,
                    )

                order.zone = zone_data.data.get("zone", "E")
                order.sub_status = "new"

                # -------------------------------
                # COMMIT CHANGES
                # -------------------------------
                db.add(order)  # ensure order is attached to session
                await db.commit()
                await db.refresh(order)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order updated successfully",
                    status=True,
                )

        except DatabaseError as e:
            logger.error(msg=f"DB Error: {str(e)}", extra=context_user_data.get())
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database error",
            )

        except Exception as e:
            logger.error(msg=f"Unhandled: {str(e)}", extra=context_user_data.get())
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Internal server error",
            )

    @staticmethod
    async def delete_order(order_id: str) -> GenericResponseModel:
        try:
            async with get_db_session() as db:
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # Fetch the order
                stmt = select(Order).where(
                    and_(
                        Order.order_id == order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                )
                result = await db.execute(stmt)
                order = result.scalar_one_or_none()

                if order is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        data={"order_id": order_id},
                        message="Order does not exist",
                    )

                if order.status not in ["new", "cancelled"]:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order cannot be deleted",
                    )

                # Soft delete
                order.is_deleted = True
                db.add(order)

                await db.commit()
                await db.refresh(order)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order deleted successfully",
                    status=True,
                )

        except DatabaseError as e:
            logger.error(extra=context_user_data.get(), msg=f"DB Error: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database error while deleting order",
            )

        except Exception as e:
            logger.error(extra=context_user_data.get(), msg=f"Unhandled error: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Internal server error while deleting order",
            )

    @staticmethod
    async def clone_order(order_id: str):
        try:
            async with get_db_session() as db:  # <-- FIXED

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # --- Fetch existing order ---
                result = await db.execute(
                    select(Order).where(
                        Order.order_id == order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                )
                order = result.scalars().first()

                if not order:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order does not exist",
                        data={"order_id": order_id},
                    )

                # --- Increase clone count ---
                order.clone_order_count += 1
                await db.commit()
                await db.refresh(order)

                clone_order_count = order.clone_order_count
                new_order_id = f"{order_id}_{clone_order_count}"

                # --- Check duplicate ---
                dup = await db.execute(
                    select(Order).where(
                        Order.order_id == new_order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                )
                if dup.scalars().first():
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Duplicate order ID generated.",
                        data={"order_id": new_order_id},
                    )

                # --- Convert original order to dictionary ---
                order_dict = order.to_model().model_dump()

                validated = cloneOrderModel(**order_dict).model_dump()

                # Remove courier key
                validated.pop("courier", None)

                # --- Create cloned order ---
                cloned_order = Order(**validated)
                cloned_order.order_id = new_order_id
                cloned_order.status = "new"
                cloned_order.sub_status = "new"
                cloned_order.tracking_info = []
                cloned_order.action_history = []

                db.add(cloned_order)
                await db.commit()
                await db.refresh(cloned_order)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Order cloned successfully",
                    data={"new_order_id": new_order_id},
                )

        except Exception as e:
            logger.error(f"Clone order error: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to clone order",
                data=str(e),
            )

    @staticmethod
    def cancel_order(order_id: str):

        try:
            with get_db_session() as db:

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # Find the existing order from the db
                order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                    .first()
                )

                # if order not found, throw an error
                if order is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order does not exist",
                    )

                if order.status != "new":
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order cannot be cancelled",
                    )

                # update the status
                order.status = "cancelled"
                order.sub_status = "cancelled"

                # Commit the updated order to the database
                db.add(order)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order cancelled Successfully",
                    status=True,
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
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

        finally:
            if db:
                db.close()

    @staticmethod
    def bulk_cancel_order(order_ids: List[str]):

        try:
            print("dkjvsdjkvnskdjvnskdnc")
            print(order_ids)

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            for order_id in order_ids:

                # Find the existing order from the db
                order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                    .first()
                )

                # if order not found, throw an error
                if order is None:
                    continue

                if order.status != "new":
                    continue

                # update the status
                order.status = "cancelled"
                order.sub_status = "cancelled"

                # Commit the updated order to the database
                db.add(order)
                db.flush()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders cancelled Successfully",
                status=True,
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
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
    def bulk_import(
        orders: List[dict],  # Accept raw dict data instead of validated models
    ):

        import time

        start_time = time.time()

        try:
            db = get_db_session()
            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # Performance tracking
            validation_start = time.time()
            validation_time = 0
            insert_time = 0

            # Initialize tracking variables
            total_orders = len(orders)
            valid_orders = []
            validation_errors = []

            print(f"🚀 Starting bulk import for {total_orders} orders")
            overall_start = time.time()

            # LIVE DB OPTIMIZATION: Configure database session for bulk operations
            db_opt_start = time.time()
            try:
                # Disable autoflush for better batch performance
                db.autoflush = False

                # Set isolation level for better concurrency (if supported)
                db.execute(
                    text("SET LOCAL synchronous_commit = OFF")
                )  # PostgreSQL optimization

                # Additional live DB optimizations
                db.execute(
                    text("SET LOCAL checkpoint_segments = 32")
                )  # Reduce checkpoint overhead
                db.execute(
                    text("SET LOCAL wal_buffers = '16MB'")
                )  # Increase WAL buffer
                db_opt_end = time.time()
                print(
                    f"✅ Database optimizations applied in {db_opt_end - db_opt_start:.3f} seconds"
                )
            except Exception as db_opt_error:
                db_opt_end = time.time()
                print(
                    f"⚠️ Database optimization warning (took {db_opt_end - db_opt_start:.3f}s): {str(db_opt_error)}"
                )

            # Step 1: Optimized pickup locations loading with caching
            pickup_start = time.time()
            pickup_locations = {}
            try:
                # Use generator expression for memory efficiency
                pickup_query = db.query(
                    Pickup_Location.location_code, Pickup_Location.pincode
                ).filter(Pickup_Location.client_id == client_id)

                # Convert to dict with string pincode for consistency
                pickup_locations = {
                    loc.location_code: str(loc.pincode) for loc in pickup_query.all()
                }
                pickup_end = time.time()
                print(
                    f"📍 Step 1: Loaded {len(pickup_locations)} pickup locations in {pickup_end - pickup_start:.3f} seconds"
                )
            except Exception as e:
                pickup_end = time.time()
                print(
                    f"❌ Error loading pickup locations (took {pickup_end - pickup_start:.3f}s): {str(e)}"
                )
                return BulkImportResponseModel(
                    success=False,
                    message="Failed to load pickup locations",
                    validation_errors=[],
                )

            # Step 2: Optimized duplicate checking with batch processing
            duplicate_start = time.time()
            order_ids_to_check = [
                order.get("order_id") for order in orders if order.get("order_id")
            ]
            existing_order_ids = set()
            if order_ids_to_check:
                print(
                    f"🔍 Step 2: Checking {len(order_ids_to_check)} order IDs for duplicates..."
                )
                # Use chunked queries for better performance
                chunk_size = 5000  # Larger chunks for better performance
                chunks_processed = 0
                for i in range(0, len(order_ids_to_check), chunk_size):
                    chunk_start = time.time()
                    chunk_ids = order_ids_to_check[i : i + chunk_size]
                    existing_chunk = (
                        db.query(Order.order_id)
                        .filter(
                            Order.order_id.in_(chunk_ids),
                            Order.company_id == company_id,
                            Order.client_id == client_id,
                        )
                        .all()
                    )
                    existing_order_ids.update(
                        {order.order_id for order in existing_chunk}
                    )
                    chunks_processed += 1
                    chunk_end = time.time()
                    print(
                        f"   📦 Chunk {chunks_processed}: {len(chunk_ids)} IDs checked in {chunk_end - chunk_start:.3f}s, found {len(existing_chunk)} duplicates"
                    )

                duplicate_end = time.time()
                print(
                    f"✅ Step 2: Duplicate check completed in {duplicate_end - duplicate_start:.3f} seconds. Found {len(existing_order_ids)} existing orders"
                )
            else:
                duplicate_end = time.time()
                print(
                    f"⚠️ Step 2: No order IDs to check for duplicates ({duplicate_end - duplicate_start:.3f}s)"
                )

            # Step 3: High-Performance Validation with Multiple Optimizations
            validation_start = time.time()
            print(f"🔬 Step 3: Starting validation for {total_orders} orders...")

            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

            # CRITICAL OPTIMIZATION: Bulk fetch all pincode mappings to avoid 6000+ individual queries
            print(f"   🗺️ Loading pincode mappings for zone calculation...")
            pincode_start = time.time()

            # Collect all unique pincodes from orders
            all_pincodes = set()
            for order in orders:
                if order.get("consignee_pincode"):
                    all_pincodes.add(str(order["consignee_pincode"]))
            # Add pickup pincodes
            for pincode in pickup_locations.values():
                all_pincodes.add(str(pincode))

            # Bulk fetch pincode mappings in chunks to avoid memory issues
            pincode_mappings = {}
            chunk_size = 1000
            pincode_list = list(all_pincodes)

            for i in range(0, len(pincode_list), chunk_size):
                chunk_pincodes = pincode_list[i : i + chunk_size]
                chunk_mappings = (
                    db.query(
                        Pincode_Mapping.pincode,
                        Pincode_Mapping.city,
                        Pincode_Mapping.state,
                    )
                    .filter(Pincode_Mapping.pincode.in_(chunk_pincodes))
                    .all()
                )

                for mapping in chunk_mappings:
                    pincode_mappings[str(mapping.pincode)] = {
                        "city": mapping.city.lower() if mapping.city else "",
                        "state": mapping.state.lower() if mapping.state else "",
                    }

            pincode_end = time.time()
            print(
                f"   ✅ Loaded {len(pincode_mappings)} pincode mappings in {pincode_end - pincode_start:.3f}s"
            )

            # Pre-compiled validation data structures for maximum speed
            zone_cache = {}
            pickup_pincodes = {code: data for code, data in pickup_locations.items()}

            # Pre-compile validation constants
            VALIDATION_CONSTANTS = {
                "client_id": client_id,
                "company_id": company_id,
                "order_type": "B2C",
                "status": "new",
                "sub_status": "new",
                "current_timestamp": current_timestamp,
                "action_history": [
                    {
                        "timestamp": current_timestamp,
                        "message": "Order Created on Platform",
                        "user_data": "",
                    }
                ],
            }  # Progress tracking for validation
            orders_processed = 0
            validation_errors_count = 0
            last_progress_time = time.time()

            # Linear validation - process each order directly
            for idx, order_data in enumerate(orders):
                # Progress reporting every 500 orders
                if idx > 0 and idx % 500 == 0:
                    current_time = time.time()
                    elapsed = current_time - last_progress_time
                    print(
                        f"   📊 Processed {idx}/{total_orders} orders (last 500 in {elapsed:.3f}s, {validation_errors_count} errors so far)"
                    )
                    last_progress_time = current_time

                # Quick type check - skip expensive validation if basic structure is wrong
                if not isinstance(order_data, dict) or not order_data.get("order_id"):
                    validation_errors.append(
                        BulkImportValidationError(
                            order_id=f"Row_{idx+1}",
                            error_type="invalid_structure",
                            error_message="Invalid order structure or missing order_id",
                            field_name="order_id",
                        )
                    )
                    validation_errors_count += 1
                    continue

                try:
                    # OPTIMIZATION 1: Fast-fail validation with minimal object creation
                    # Skip Pydantic validation for obviously invalid data
                    required_fields = [
                        "order_id",
                        "consignee_full_name",
                        "consignee_phone",
                        "consignee_address",
                        "consignee_pincode",
                        "consignee_city",
                        "consignee_state",
                        "consignee_country",
                        "pickup_location_code",
                        "payment_mode",
                        "total_amount",
                        "order_value",
                        "products",
                        "length",
                        "breadth",
                        "height",
                        "weight",
                    ]

                    missing_fields = [
                        field for field in required_fields if not order_data.get(field)
                    ]
                    if missing_fields:
                        validation_errors.append(
                            BulkImportValidationError(
                                order_id=order_data.get("order_id", f"Row_{idx+1}"),
                                error_type="missing_fields",
                                error_message=f"Missing required fields: {', '.join(missing_fields)}",
                                field_name=missing_fields[0],
                            )
                        )
                        continue

                    # OPTIMIZATION 2: Skip Pydantic validation and do manual validation
                    # Only use Pydantic if we need the model_dump functionality
                    order_dict = order_data.copy()  # Shallow copy is much faster

                    # Remove courier field if present
                    order_dict.pop(
                        "courier", None
                    )  # More efficient than del with check

                    # OPTIMIZATION 3: Fast duplicate check
                    order_id = order_dict["order_id"]
                    if order_id in existing_order_ids:
                        validation_errors.append(
                            BulkImportValidationError(
                                order_id=order_id,
                                error_type="duplicate_order",
                                error_message="Duplicate order ID",
                                field_name="order_id",
                            )
                        )
                        continue

                    # OPTIMIZATION 4: Fast pickup location validation
                    pickup_location_code = order_dict.get("pickup_location_code")
                    if pickup_location_code not in pickup_pincodes:
                        validation_errors.append(
                            BulkImportValidationError(
                                order_id=order_id,
                                error_type="invalid_pickup_location",
                                error_message="Invalid pickup location code",
                                field_name="pickup_location_code",
                            )
                        )
                        continue

                    # OPTIMIZATION 5: Super-fast zone calculation using preloaded mappings
                    pickup_pincode = pickup_pincodes[pickup_location_code]
                    consignee_pincode = str(order_dict["consignee_pincode"])
                    zone_key = f"{pickup_pincode}_{consignee_pincode}"

                    if zone_key in zone_cache:
                        zone = zone_cache[zone_key]
                    else:
                        # Calculate zone using preloaded mappings (NO DATABASE CALLS!)
                        pickup_mapping = pincode_mappings.get(str(pickup_pincode))
                        consignee_mapping = pincode_mappings.get(consignee_pincode)

                        if not pickup_mapping or not consignee_mapping:
                            zone = "D"  # Default zone
                        else:
                            # Zone calculation logic (same as ShipmentService but without DB calls)
                            # A Zone: Same city
                            if pickup_mapping["city"] == consignee_mapping["city"]:
                                zone = "A"
                            # B Zone: Same state
                            elif pickup_mapping["state"] == consignee_mapping["state"]:
                                zone = "B"
                            # E Zone: Special zones
                            elif pickup_mapping["state"] in [
                                s.lower() for s in special_zone
                            ] or consignee_mapping["state"] in [
                                s.lower() for s in special_zone
                            ]:
                                zone = "E"
                            # C Zone: Metro to Metro
                            elif pickup_mapping["city"] in [
                                c.lower() for c in metro_cities
                            ] and consignee_mapping["city"] in [
                                c.lower() for c in metro_cities
                            ]:
                                zone = "C"
                            # Default: D Zone
                            else:
                                zone = "D"

                        zone_cache[zone_key] = zone

                        # OPTIMIZATION 6: Pre-populate cache for common patterns
                        if len(zone_cache) < 1000:  # Prevent memory overflow
                            # Cache similar pickup pincode combinations
                            for existing_key in list(zone_cache.keys())[:10]:
                                if existing_key.startswith(str(pickup_pincode) + "_"):
                                    # Found similar pattern, can potentially reuse
                                    break

                    # OPTIMIZATION 7: Fast numeric conversion with error handling
                    try:
                        # Convert all at once with list comprehension - faster than individual calls
                        length, breadth, height, weight = [
                            float(order_dict[field])
                            for field in ["length", "breadth", "height", "weight"]
                        ]

                        # Pre-calculate both weights in one operation
                        volumetric_weight = round((length * breadth * height) / 5000, 3)
                        applicable_weight = round(max(weight, volumetric_weight), 3)

                    except (ValueError, TypeError, KeyError):
                        validation_errors.append(
                            BulkImportValidationError(
                                order_id=order_id,
                                error_type="weight_calculation",
                                error_message="Invalid dimensions or weight values",
                                field_name="weight/dimensions",
                            )
                        )
                        continue

                    # OPTIMIZATION 8: Fast product quantity calculation
                    try:
                        products = order_dict.get("products", [])
                        product_quantity = (
                            sum(p.get("quantity", 0) for p in products)
                            if products
                            else 1
                        )
                    except (TypeError, AttributeError):
                        product_quantity = 1

                    # OPTIMIZATION 9: Bulk update with pre-computed constants
                    order_dict.update(VALIDATION_CONSTANTS)
                    order_dict.update(
                        {
                            "zone": zone,
                            "applicable_weight": applicable_weight,
                            "volumetric_weight": volumetric_weight,
                            "product_quantity": product_quantity,
                        }
                    )

                    # Add to valid orders
                    valid_orders.append(order_dict)
                    existing_order_ids.add(
                        order_id
                    )  # Prevent duplicates within the same upload

                except Exception as e:
                    validation_errors.append(
                        BulkImportValidationError(
                            order_id=order_data.get("order_id", f"Row_{idx+1}"),
                            error_type="processing_error",
                            error_message=f"Order processing failed: {str(e)}",
                            field_name=None,
                        )
                    )
                    validation_errors_count += 1
                    continue

            validation_end = time.time()
            validation_time = validation_end - validation_start

            print(f"✅ Step 3: Validation completed in {validation_time:.3f} seconds")
            print(
                f"   📈 Results: {len(valid_orders)} valid orders, {len(validation_errors)} errors"
            )
            print(f"   ⚡ Performance: {len(orders)/validation_time:.1f} orders/second")
            if zone_cache:
                print(f"   🗂️ Zone cache: {len(zone_cache)} entries created")

            # Step 4: Optimized bulk insert using raw SQL for maximum performance
            insert_start = time.time()
            print(
                f"💾 Step 4: Starting database insertion for {len(valid_orders)} valid orders..."
            )

            successful_orders = 0
            if valid_orders:
                try:
                    # Use raw SQL bulk insert for maximum performance
                    from sqlalchemy import text

                    # Prepare base timestamp for created_at and updated_at
                    current_timestamp_db = datetime.now()

                    # Create optimized batch size for live DB (smaller batches for network efficiency)
                    batch_size = 500  # Reduced from 1000 for better live DB performance
                    total_batches = (len(valid_orders) + batch_size - 1) // batch_size
                    print(
                        f"   📦 Processing {total_batches} batches of {batch_size} orders each"
                    )

                    for i in range(0, len(valid_orders), batch_size):
                        batch_start = time.time()
                        batch_num = (i // batch_size) + 1
                        batch_orders = valid_orders[i : i + batch_size]
                        print(
                            f"   🔨 Batch {batch_num}/{total_batches}: Preparing {len(batch_orders)} orders..."
                        )

                        # Prepare values for bulk insert
                        prep_start = time.time()
                        insert_values = []
                        for order_dict in batch_orders:
                            try:
                                # Convert datetime objects to strings if needed
                                order_date_str = order_dict.get("order_date")
                                if hasattr(order_date_str, "strftime"):
                                    order_date_str = order_date_str.strftime(
                                        "%Y-%m-%d %H:%M:%S%z"
                                    )

                                # Convert JSON fields to strings
                                import json
                                import uuid

                                action_history_json = json.dumps(
                                    order_dict.get("action_history", [])
                                )
                                order_tags_json = json.dumps(
                                    order_dict.get("order_tags", [])
                                )

                                # Helper function to safely convert to float
                                def safe_float(value, default=None):
                                    if value is None or value == "":
                                        return default
                                    try:
                                        return float(value)
                                    except (ValueError, TypeError):
                                        return default

                                # Helper function to safely convert to int
                                def safe_int(value, default=0):
                                    if value is None or value == "":
                                        return default
                                    try:
                                        return int(value)
                                    except (ValueError, TypeError):
                                        return default

                                # Clean products: ensure numeric types for quantity and unit_price
                                raw_products = order_dict.get("products", []) or []
                                cleaned_products = []
                                computed_product_quantity = 0
                                for prod in raw_products:
                                    try:
                                        # Work on a shallow copy to avoid mutating input unexpectedly
                                        p = dict(prod)
                                        qty = safe_int(p.get("quantity"), 0)
                                        price = safe_float(p.get("unit_price"), None)
                                        p["quantity"] = qty
                                        # Only set unit_price if it's present or convertible
                                        if price is not None:
                                            # Format to a float (keeps numeric type in JSON)
                                            p["unit_price"] = price
                                        else:
                                            # Remove empty unit_price to avoid storing as empty string
                                            p.pop("unit_price", None)
                                        cleaned_products.append(p)
                                        computed_product_quantity += qty
                                    except Exception:
                                        # Fallback: keep original product if cleaning fails for this item
                                        cleaned_products.append(prod)

                                products_json = json.dumps(cleaned_products)

                                # Prepare the insert tuple with all required fields
                                insert_values.append(
                                    (
                                        # Database required fields
                                        uuid.uuid4(),  # uuid
                                        # Basic order info
                                        str(order_dict.get("order_id", "")),
                                        str(order_dict.get("order_type", "B2C")),
                                        order_date_str,
                                        order_dict.get("channel"),
                                        safe_int(order_dict.get("company_id")),
                                        safe_int(order_dict.get("client_id")),
                                        # Consignee details
                                        str(order_dict.get("consignee_full_name", "")),
                                        str(order_dict.get("consignee_phone", "")),
                                        order_dict.get("consignee_alternate_phone"),
                                        order_dict.get("consignee_email"),
                                        order_dict.get("consignee_company"),
                                        order_dict.get("consignee_gstin"),
                                        str(order_dict.get("consignee_address", "")),
                                        order_dict.get("consignee_landmark"),
                                        str(order_dict.get("consignee_pincode", "")),
                                        str(order_dict.get("consignee_city", "")),
                                        str(order_dict.get("consignee_state", "")),
                                        str(order_dict.get("consignee_country", "")),
                                        # Billing details
                                        bool(
                                            order_dict.get(
                                                "billing_is_same_as_consignee", True
                                            )
                                        ),
                                        order_dict.get("billing_full_name"),
                                        order_dict.get("billing_phone"),
                                        order_dict.get("billing_email"),
                                        order_dict.get("billing_address"),
                                        order_dict.get("billing_landmark"),
                                        order_dict.get("billing_pincode"),
                                        order_dict.get("billing_city"),
                                        order_dict.get("billing_state"),
                                        order_dict.get("billing_country"),
                                        # Pickup and payment
                                        str(order_dict.get("pickup_location_code", "")),
                                        str(order_dict.get("payment_mode", "")),
                                        safe_float(order_dict.get("total_amount"), 0.0),
                                        safe_float(order_dict.get("order_value"), 0.0),
                                        safe_float(order_dict.get("shipping_charges")),
                                        safe_float(order_dict.get("cod_charges")),
                                        safe_float(order_dict.get("discount")),
                                        safe_float(order_dict.get("gift_wrap_charges")),
                                        safe_float(order_dict.get("other_charges")),
                                        safe_float(order_dict.get("tax_amount")),
                                        # Invoice details
                                        order_dict.get("invoice_number"),
                                        order_dict.get("invoice_date"),
                                        safe_float(order_dict.get("invoice_amount")),
                                        order_dict.get("eway_bill_number"),
                                        # Products and package
                                        products_json,
                                        safe_int(order_dict.get("product_quantity"), 1),
                                        safe_float(order_dict.get("length"), 0.0),
                                        safe_float(order_dict.get("breadth"), 0.0),
                                        safe_float(order_dict.get("height"), 0.0),
                                        safe_float(order_dict.get("weight"), 0.0),
                                        safe_float(
                                            order_dict.get("applicable_weight"), 0.0
                                        ),
                                        safe_float(
                                            order_dict.get("volumetric_weight"), 0.0
                                        ),
                                        # Status and zone
                                        str(order_dict.get("zone", "")),
                                        str(order_dict.get("status", "new")),
                                        str(order_dict.get("sub_status", "new")),
                                        # JSON fields
                                        action_history_json,
                                        order_tags_json,
                                        # Default values
                                        0,  # clone_order_count
                                        0,  # cancel_count
                                        False,  # is_label_generated
                                        False,  # is_deleted
                                        current_timestamp_db,  # created_at
                                        current_timestamp_db,  # updated_at
                                        current_timestamp_db,  # last_update_date
                                    )
                                )
                            except Exception as e:
                                validation_errors.append(
                                    BulkImportValidationError(
                                        order_id=order_dict.get("order_id", "unknown"),
                                        error_type="data_preparation",
                                        error_message=f"Data preparation failed: {str(e)}",
                                        field_name=None,
                                    )
                                )
                                continue

                        prep_end = time.time()
                        print(
                            f"      🛠️ Data preparation: {len(insert_values)} orders prepared in {prep_end - prep_start:.3f}s"
                        )

                        # Execute bulk insert if we have valid data
                        if insert_values:
                            try:
                                # LIVE DB OPTIMIZATION: True bulk insert with single SQL statement
                                # This reduces 1000 network calls to just 1 call

                                # Build VALUES clause for bulk insert
                                sql_build_start = time.time()
                                values_placeholders = []
                                all_params = {}

                                for idx, value_tuple in enumerate(insert_values):
                                    # Create unique parameter names for this row
                                    row_params = []
                                    for field_idx, value in enumerate(value_tuple):
                                        param_name = f"p{idx}_{field_idx}"
                                        row_params.append(f":{param_name}")
                                        all_params[param_name] = value

                                    values_placeholders.append(
                                        f"({', '.join(row_params)})"
                                    )

                                # Build the complete bulk insert SQL
                                bulk_insert_sql = f"""
                                INSERT INTO "order" (
                                    uuid, order_id, order_type, order_date, channel, company_id, client_id,
                                    consignee_full_name, consignee_phone, consignee_alternate_phone, consignee_email,
                                    consignee_company, consignee_gstin, consignee_address, consignee_landmark,
                                    consignee_pincode, consignee_city, consignee_state, consignee_country,
                                    billing_is_same_as_consignee, billing_full_name, billing_phone, billing_email,
                                    billing_address, billing_landmark, billing_pincode, billing_city, billing_state, billing_country,
                                    pickup_location_code, payment_mode, total_amount, order_value,
                                    shipping_charges, cod_charges, discount, gift_wrap_charges, other_charges, tax_amount,
                                    invoice_number, invoice_date, invoice_amount, eway_bill_number,
                                    products, product_quantity, length, breadth, height, weight, applicable_weight, volumetric_weight,
                                    zone, status, sub_status, action_history, order_tags,
                                    clone_order_count, cancel_count, is_label_generated, is_deleted, created_at, updated_at, last_update_date
                                ) VALUES {', '.join(values_placeholders)}
                                """

                                sql_build_end = time.time()
                                print(
                                    f"      🏗️ SQL building: {sql_build_end - sql_build_start:.3f}s ({len(all_params)} parameters)"
                                )

                                # Execute single bulk insert - MASSIVE performance improvement for live DB
                                bulk_start = time.time()
                                print(
                                    f"      🚀 Executing bulk insert for {len(insert_values)} orders..."
                                )
                                result = db.execute(text(bulk_insert_sql), all_params)
                                bulk_end = time.time()
                                successful_orders += len(insert_values)

                                batch_end = time.time()
                                batch_total = batch_end - batch_start
                                print(
                                    f"      ✅ Batch {batch_num}/{total_batches}: {len(insert_values)} orders inserted in {bulk_end - bulk_start:.3f}s (total batch: {batch_total:.3f}s)"
                                )

                            except Exception as e:
                                # If bulk insert fails, add all orders to validation errors
                                batch_end = time.time()
                                print(
                                    f"      ❌ Batch {batch_num}/{total_batches} failed in {batch_end - batch_start:.3f}s: {str(e)}"
                                )
                                for order_dict in batch_orders:
                                    validation_errors.append(
                                        BulkImportValidationError(
                                            order_id=order_dict.get(
                                                "order_id", "unknown"
                                            ),
                                            error_type="insert_error",
                                            error_message=f"Database insert failed: {str(e)}",
                                            field_name=None,
                                        )
                                    )

                    # Single commit for all batches - critical for live DB performance
                    if successful_orders > 0:
                        commit_start = time.time()
                        db.commit()
                        commit_end = time.time()
                        print(
                            f"   💾 Committed {successful_orders} orders to database in {commit_end - commit_start:.3f}s"
                        )

                except Exception as e:
                    rollback_start = time.time()
                    db.rollback()
                    rollback_end = time.time()
                    print(
                        f"   💥 Bulk insert failed completely in {rollback_end - rollback_start:.3f}s: {str(e)}"
                    )
                    # Add all orders to validation errors
                    for order_dict in valid_orders:
                        validation_errors.append(
                            BulkImportValidationError(
                                order_id=order_dict.get("order_id", "unknown"),
                                error_type="insert_error",
                                error_message=f"Complete insert failure: {str(e)}",
                                field_name=None,
                            )
                        )

            insert_end = time.time()
            insert_time = insert_end - insert_start

            print(
                f"✅ Step 4: Database insertion completed in {insert_time:.3f} seconds"
            )
            print(f"   📈 Results: {successful_orders} orders inserted successfully")
            if successful_orders > 0:
                print(
                    f"   ⚡ Performance: {successful_orders/insert_time:.1f} orders/second"
                )

            # Step 5: Handle error orders - create Excel file using ErrorExcelGenerator
            error_start = time.time()
            error_file_url = None
            failed_orders = len(validation_errors)

            if validation_errors:
                print(f"📋 Step 5: Processing {failed_orders} validation errors...")
                try:
                    # Convert validation errors to format compatible with ErrorExcelGenerator
                    error_data_for_excel = []

                    for error in validation_errors:
                        # Find original order data
                        original_order = next(
                            (
                                order
                                for order in orders
                                if order.get("order_id") == error.order_id
                            ),
                            {},
                        )

                        # Create user-friendly error mapping
                        error_message_map = {
                            "duplicate_order": "Order ID already exists in the system",
                            "invalid_pickup_location": "Pickup location code not found",
                            "zone_calculation": "Unable to calculate shipping zone for this pincode",
                            "processing_error": "Order processing failed",
                            "model_creation": "Database model creation failed",
                            "insert_error": "Failed to save order to database",
                        }

                        # Create suggested fixes
                        suggested_fix_map = {
                            "schema_validation": "Check the specific field mentioned in the error description",
                            "duplicate_order": "Use a unique order ID",
                            "invalid_pickup_location": "Use valid pickup location code from your account",
                            "zone_calculation": "Verify pincode is correct and serviceable",
                            "processing_error": "Check all field values are correct",
                            "model_creation": "Contact support if issue persists",
                            "insert_error": "Contact support if issue persists",
                        }

                        # Map original order data to the correct field names for ErrorExcelGenerator
                        formatted_order = {
                            # Map the original order fields to match our frontend validation
                            "order_id": original_order.get("order_id", ""),
                            "order_date": original_order.get("order_date", ""),
                            "channel": original_order.get("channel", ""),
                            "consignee_full_name": original_order.get(
                                "consignee_full_name", ""
                            ),
                            "consignee_phone": original_order.get(
                                "consignee_phone", ""
                            ),
                            "consignee_email": original_order.get(
                                "consignee_email", ""
                            ),
                            "consignee_alternate_phone": original_order.get(
                                "consignee_alternate_phone", ""
                            ),
                            "consignee_company": original_order.get(
                                "consignee_company", ""
                            ),
                            "consignee_gstin": original_order.get(
                                "consignee_gstin", ""
                            ),
                            "consignee_address": original_order.get(
                                "consignee_address", ""
                            ),
                            "consignee_landmark": original_order.get(
                                "consignee_landmark", ""
                            ),
                            "consignee_pincode": original_order.get(
                                "consignee_pincode", ""
                            ),
                            "consignee_city": original_order.get("consignee_city", ""),
                            "consignee_state": original_order.get(
                                "consignee_state", ""
                            ),
                            "consignee_country": original_order.get(
                                "consignee_country", ""
                            ),
                            "billing_is_same_as_consignee": original_order.get(
                                "billing_is_same_as_consignee", ""
                            ),
                            "billing_full_name": original_order.get(
                                "billing_full_name", ""
                            ),
                            "billing_phone": original_order.get("billing_phone", ""),
                            "billing_email": original_order.get("billing_email", ""),
                            "billing_address": original_order.get(
                                "billing_address", ""
                            ),
                            "billing_landmark": original_order.get(
                                "billing_landmark", ""
                            ),
                            "billing_pincode": original_order.get(
                                "billing_pincode", ""
                            ),
                            "billing_city": original_order.get("billing_city", ""),
                            "billing_state": original_order.get("billing_state", ""),
                            "billing_country": original_order.get(
                                "billing_country", ""
                            ),
                            "pickup_location_code": original_order.get(
                                "pickup_location_code", ""
                            ),
                            # Extract product information (assuming first product for simplicity)
                            "name": (
                                original_order.get("products", [{}])[0].get("name", "")
                                if original_order.get("products")
                                else ""
                            ),
                            "unit_price": (
                                original_order.get("products", [{}])[0].get(
                                    "unit_price", ""
                                )
                                if original_order.get("products")
                                else ""
                            ),
                            "quantity": (
                                original_order.get("products", [{}])[0].get(
                                    "quantity", ""
                                )
                                if original_order.get("products")
                                else ""
                            ),
                            "sku_code": (
                                original_order.get("products", [{}])[0].get(
                                    "sku_code", ""
                                )
                                if original_order.get("products")
                                else ""
                            ),
                            "length": original_order.get("length", ""),
                            "breadth": original_order.get("breadth", ""),
                            "height": original_order.get("height", ""),
                            "weight": original_order.get("weight", ""),
                            "payment_mode": original_order.get("payment_mode", ""),
                            "shipping_charges": original_order.get(
                                "shipping_charges", ""
                            ),
                            "cod_charges": original_order.get("cod_charges", ""),
                            "discount": original_order.get("discount", ""),
                            "gift_wrap_charges": original_order.get(
                                "gift_wrap_charges", ""
                            ),
                            "other_charges": original_order.get("other_charges", ""),
                            "tax_amount": original_order.get("tax_amount", ""),
                            # Error information
                            "error_fields": (
                                [error.field_name] if error.field_name else []
                            ),
                            "error_description": (
                                error.error_message
                                if error.error_type == "schema_validation"
                                else error_message_map.get(
                                    error.error_type, error.error_message
                                )
                            ),
                            "error_field": error.field_name or "general",
                            "suggested_fix": suggested_fix_map.get(
                                error.error_type, "Check field values and try again"
                            ),
                        }

                        error_data_for_excel.append(formatted_order)

                    # Generate Excel file using ErrorExcelGenerator
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"bulk_upload_validation_errors_{timestamp}.xlsx"

                    excel_base64 = ErrorExcelGenerator.generate_error_excel(
                        error_data_for_excel, filename
                    )

                    # Convert base64 to BytesIO for S3 upload
                    excel_buffer = BytesIO(base64.b64decode(excel_base64))

                    # Upload to S3
                    s3_key = f"{client_id}/bulk_upload_errors/{filename}"

                    upload_result = upload_file_to_s3(
                        file_obj=excel_buffer,
                        s3_key=s3_key,
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                    if upload_result.get("success"):
                        error_file_url = upload_result.get("url")

                    error_end = time.time()
                    print(
                        f"✅ Step 5: Error file generated and uploaded in {error_end - error_start:.3f} seconds"
                    )

                except Exception as e:
                    error_end = time.time()
                    print(
                        f"❌ Step 5: Failed to create/upload error file in {error_end - error_start:.3f}s: {str(e)}"
                    )
            else:
                error_end = time.time()
                print(
                    f"✅ Step 5: No errors to process ({error_end - error_start:.3f}s)"
                )

            # Step 6: Create bulk upload log entry
            log_start = time.time()
            try:
                bulk_log = BulkOrderUploadLogs(
                    order_count=total_orders,
                    uploaded_order_count=successful_orders,
                    error_order_count=failed_orders,
                    error_file_url=error_file_url,
                    client_id=client_id,
                )

                db.add(bulk_log)
                db.commit()
                log_end = time.time()
                print(
                    f"✅ Step 6: Bulk upload log created in {log_end - log_start:.3f} seconds"
                )

            except Exception as e:
                log_end = time.time()
                print(
                    f"❌ Step 6: Failed to create bulk upload log in {log_end - log_start:.3f}s: {str(e)}"
                )

            # Step 7: Return response with performance metrics
            overall_end = time.time()
            total_time = overall_end - overall_start

            print(f"\n🎯 BULK IMPORT COMPLETE!")
            print(f"📊 Overall Performance Summary:")
            print(f"   ⏱️ Total time: {total_time:.3f} seconds")
            print(
                f"   📋 Validation: {validation_time:.3f}s ({(validation_time/total_time)*100:.1f}%)"
            )
            print(
                f"   💾 Database: {insert_time:.3f}s ({(insert_time/total_time)*100:.1f}%)"
            )
            print(f"   📄 Error processing: {error_end - error_start:.3f}s")
            print(f"   📝 Logging: {log_end - log_start:.3f}s")
            print(f"   📈 Success rate: {(successful_orders/total_orders)*100:.1f}%")
            print(
                f"   ⚡ Overall throughput: {total_orders/total_time:.1f} orders/second"
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message=f"{successful_orders} orders uploaded successfully, {failed_orders} orders failed.",
                status=True,
                data=BulkImportResponseModel(
                    total_orders=total_orders,
                    successful_orders=successful_orders,
                    failed_orders=failed_orders,
                ),
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
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
    async def get_all_orders(order_filters: Order_filters):

        # -----------------------
        # FIX: asyncpg datetime handling
        # -----------------------
        def to_naive(dt):
            if dt is None:
                return None
            # force convert → UTC → strip tzinfo
            return dt.astimezone(timezone.utc).replace(tzinfo=None)

        db: AsyncSession = get_db_session()

        try:
            # Destructure filters
            page_number = order_filters.page_number
            batch_size = order_filters.batch_size
            order_status = order_filters.order_status
            current_status = order_filters.current_status
            search_term = order_filters.search_term

            # FIX: Make datetimes naive
            start_date = to_naive(order_filters.start_date)
            end_date = to_naive(order_filters.end_date)

            date_type = order_filters.date_type
            tags = order_filters.tags
            repeat_customer = order_filters.repeat_customer
            payment_mode = order_filters.payment_mode
            courier_filter = order_filters.courier_filter
            sku_codes = order_filters.sku_codes
            product_name = order_filters.product_name
            product_quantity = order_filters.product_quantity
            order_id = order_filters.order_id
            pincode = order_filters.pincode
            pickup_location = order_filters.pickup_location

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # --------------------------------
            # BASE FILTERS
            # --------------------------------
            base_filters = [
                Order.company_id == company_id,
                Order.client_id == client_id,
                Order.is_deleted == False,
            ]

            common_filters = []

            # --------------------------------
            # SEARCH FILTER
            # --------------------------------
            if search_term:
                terms = [t.strip() for t in search_term.split(",")]
                common_filters.append(
                    or_(
                        *[
                            or_(
                                Order.order_id == t,
                                Order.awb_number == t,
                                Order.consignee_phone == t,
                                Order.consignee_alternate_phone == t,
                                Order.consignee_email == t,
                            )
                            for t in terms
                        ]
                    )
                )

            # --------------------------------
            # DATE FILTERS (fixed)
            # --------------------------------
            if date_type == "order date":
                if start_date:
                    common_filters.append(
                        cast(Order.order_date, DateTime) >= start_date
                    )
                if end_date:
                    common_filters.append(cast(Order.order_date, DateTime) <= end_date)

            elif date_type == "booking date":
                if start_date:
                    common_filters.append(
                        cast(Order.booking_date, DateTime) >= start_date
                    )
                if end_date:
                    common_filters.append(
                        cast(Order.booking_date, DateTime) <= end_date
                    )

            # --------------------------------
            # REMAINING FILTERS
            # --------------------------------
            remaining_filters = []
            sku_params = {}

            if current_status:
                remaining_filters.append(Order.sub_status == current_status)

            # SKU FILTER
            if sku_codes:
                sku_codes = [x.strip() for x in sku_codes.split(",")]

                like_conditions = [
                    text(
                        f"EXISTS (SELECT 1 FROM jsonb_array_elements(products) AS elem "
                        f"WHERE elem->>'sku_code' ILIKE :sku_{i})"
                    )
                    for i, _ in enumerate(sku_codes)
                ]

                remaining_filters.append(or_(*like_conditions))
                sku_params = {f"sku_{i}": f"%{sku}%" for i, sku in enumerate(sku_codes)}

            # PRODUCT NAME FILTER
            if product_name:
                names = [x.strip() for x in product_name.split(",")]
                remaining_filters.append(
                    or_(
                        *[
                            cast(Order.products, String).ilike(f'%"name": "%{n}%"%')
                            for n in names
                        ]
                    )
                )

            # PINCODE FILTER
            if pincode:
                pins = [x.strip() for x in pincode.split(",")]
                remaining_filters.append(
                    or_(*[Order.consignee_pincode == p for p in pins])
                )

            # PICKUP LOCATION
            if pickup_location:
                remaining_filters.append(Order.pickup_location_code == pickup_location)

            # ORDER ID
            if order_id:
                ids = [x.strip() for x in order_id.split(",")]
                remaining_filters.append(Order.order_id.in_(ids))

            if payment_mode:
                remaining_filters.append(Order.payment_mode == payment_mode)

            if courier_filter:
                remaining_filters.append(Order.courier_partner == courier_filter)

            if product_quantity:
                remaining_filters.append(Order.product_quantity == product_quantity)

            if tags:
                remaining_filters.append(
                    cast(Order.order_tags, String).ilike(f"%{tags}%")
                )

            # --------------------------------
            # REPEAT CUSTOMER HANDLING
            # --------------------------------
            if repeat_customer is True:
                repeat_query = (
                    select(Order.consignee_phone)
                    .where(
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        Order.consignee_phone.isnot(None),
                        Order.consignee_phone != "",
                    )
                    .group_by(Order.consignee_phone)
                    .having(func.count(Order.id) > 1)
                )
                result = await db.execute(repeat_query)
                phones = result.scalars().all()
                remaining_filters.append(Order.consignee_phone.in_(phones))

            # --------------------------------
            # STATUS COUNTS
            # --------------------------------
            status_count_query = (
                select(Order.status, func.count(Order.id))
                .where(*base_filters)
                .where(*common_filters)
                .where(*remaining_filters)
                .group_by(Order.status)
            )

            if sku_codes:
                status_count_query = status_count_query.params(**sku_params)

            status_result = await db.execute(status_count_query)
            status_counts = {status: count for status, count in status_result.all()}
            status_counts["all"] = sum(status_counts.values())

            # --------------------------------
            # MAIN QUERY
            # --------------------------------
            main_query = (
                select(Order)
                .options(joinedload(Order.pickup_location))
                .where(*base_filters, *common_filters, *remaining_filters)
                .order_by(
                    desc(Order.order_date), desc(Order.created_at), desc(Order.id)
                )
            )

            if sku_codes:
                main_query = main_query.params(**sku_params)

            if order_status != "all":
                main_query = main_query.where(Order.status == order_status)

            # --------------------------------
            # TOTAL COUNT
            # --------------------------------
            count_query = select(func.count()).select_from(
                select(Order.id)
                .where(*base_filters, *common_filters, *remaining_filters)
                .subquery()
            )
            total_count = (await db.execute(count_query)).scalar()

            # Pagination
            offset_value = (page_number - 1) * batch_size
            main_query = main_query.offset(offset_value).limit(batch_size)

            result = await db.execute(main_query)
            fetched_orders = result.scalars().all()

            # --------------------------------
            # REPEAT CUSTOMER PREVIOUS ORDERS
            # --------------------------------
            phone_numbers = [
                o.consignee_phone for o in fetched_orders if o.consignee_phone
            ]

            previous_counts = {}
            if phone_numbers:
                q = (
                    select(Order.consignee_phone, func.count(Order.id))
                    .where(
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        Order.consignee_phone.in_(phone_numbers),
                    )
                    .group_by(Order.consignee_phone)
                )
                res = await db.execute(q)
                previous_counts = dict(res.all())

            orders_response = []
            for o in fetched_orders:
                d = o.to_model().model_dump()
                phone = o.consignee_phone
                tot = previous_counts.get(phone, 1)
                d["previous_order_count"] = max(0, tot - 1)
                orders_response.append(Order_Response_Model(**d))

            return GenericResponseModel(
                status_code=200,
                message="Orders fetched Successfully",
                status=True,
                data={
                    "orders": orders_response,
                    "total_count": total_count,
                    "status_counts": status_counts,
                },
            )

        except Exception as e:
            logger.error(f"Unhandled error: {e}")
            return GenericResponseModel(
                status_code=500,
                message="An internal server error occurred.",
            )

        finally:
            if db:
                await db.close()

    @staticmethod
    async def get_easycom_token():
        """
        Hit EasyEcom API to fetch token.
        """
        try:
            EASYCOM_TOKEN_URL = "https://api.easyecom.io/getApiToken"
            payload = {"email": "jaidurgatraders866@gmail.com", "password": "Amol@2025"}

            headers = {
                "Content-Type": "application/json",
                "Cookie": "XSRF-TOKEN=xxxx; laravel_session=xxxx; PHPSESSID=xxxx",
            }

            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    EASYCOM_TOKEN_URL, json=payload, headers=headers
                )

            # print("Easycom Response:", response.text)

            if response.status_code != 200:
                return {
                    "status": False,
                    "message": "Failed to fetch token",
                    "api_response": response.text,
                }

            data = response.json()

            token = data.get("token") or data.get("api_token")  # based on API structure

            if not token:
                return {
                    "status": False,
                    "message": "Token not found in API response",
                    "api_response": data,
                }

            return {
                "status": True,
                "token": token,
                "message": "Token fetched successfully",
            }

        except Exception as e:
            return {"status": False, "message": str(e)}

    @staticmethod
    async def get_orders_from_easycom(api_token: str, start_date: str, end_date: str):
        try:
            EASYCOM_GET_ORDERS_URL = "https://api.easyecom.io/orders/V2/getAllOrders"
            # Encode dates for URL
            start_date_encoded = quote(start_date)
            end_date_encoded = quote(end_date)

            url = (
                f"{EASYCOM_GET_ORDERS_URL}"
                f"?api_token={api_token}"
                f"&start_date={start_date_encoded}"
                f"&end_date={end_date_encoded}"
            )

            headers = {
                "Cookie": "XSRF-TOKEN=xxxx; laravel_session=xxxx; PHPSESSID=xxxx"
            }

            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, headers=headers)

            data = response.json()

            return {
                "status": True,
                "message": "Orders fetched successfully",
                "orders": data,
            }

        except Exception as e:
            logging.exception("Error while fetching Easycom orders")
            return {"status": False, "message": str(e)}

    @staticmethod
    async def get_easyecom_order_details(invoice_id: str, api_token: str):
        """
        Fetch order details from EasyEcom V2 API
        """
        url = f"https://api.easyecom.io/orders/V2/getOrderDetails?api_token={api_token}&invoice_id={invoice_id}"

        headers = {"Cookie": "XSRF-TOKEN=xxxx; laravel_session=xxxx; PHPSESSID=xxxx"}

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, headers=headers)
            return response.json()

    @staticmethod
    async def add_carrier_credentials(
        api_token: str, carrier_id: int, username: str, password: str, token: str
    ):
        """
        Add or update carrier credentials in EasyEcom
        """
        url = f"https://api.easyecom.io/Credentials/addCarrierCredentials?api_token={api_token}"

        payload = {
            "carrier_id": carrier_id,
            "username": username,
            "password": password,
            "token": token,
        }

        headers = {
            "Content-Type": "application/json",
            "Cookie": ("XSRF-TOKEN=###" "laravel_session=###" "PHPSESSID=###"),
        }

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, json=payload, headers=headers)
            return response.json()

    @staticmethod
    async def assign_awb(
        api_token: str,
        invoice_id: str,
        courier: str,
        awb_num: str,
        company_carrier_id: int,
        shipping_label_path: str,  # Path to the PDF file
        invoice_url: str,
        origin_code: str,
        destination_code: str,
    ):
        """
        Assign AWB to an order in EasyEcom
        """

        # Convert PDF to base64
        with open(shipping_label_path, "rb") as f:
            shipping_label_base64 = base64.b64encode(f.read()).decode("utf-8")

        url = f"https://api.easyecom.io/Carrier/assignAWB?api_token={api_token}"

        payload = {
            "invoiceId": invoice_id,
            "courier": courier,
            "awbNum": awb_num,
            "companyCarrierId": company_carrier_id,
            "shippingLabelUrl": shipping_label_base64,
            "invoiceUrl": invoice_url,
            "origin_code": origin_code,
            "destination_code": destination_code,
        }

        headers = {
            "Content-Type": "application/json",
            "Cookie": ("XSRF-TOKEN=###" "laravel_session=###" "PHPSESSID=###"),
        }

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, json=payload, headers=headers)
            return response.json()

    @staticmethod
    async def sync_orders_from_easycom():
        # time.sleep(3)
        print("welcome to sync order")
        # GET TOKEN FROM EASYCOM THIS IS REAL TIME TOKEN BUT YOU CAN USE STATIC TOKEN AS WELL BECAUSE TOKEN VALIDITY IS LONG IF NOT VALID THEN FETCH NEW TOKEN
        # response = await OrderService.get_easycom_token()
        # print("response=>", response["api_response"]["data"]["api_token"])
        api_token = "c04b200bec738367faf360cce33a4e078f1cdce399cf7efc64da1899983e8fbd"

        start_date = "2025-11-20 00:00:00"
        end_date = "2025-11-27 00:00:00"

        result = await OrderService.get_orders_from_easycom(
            api_token, start_date, end_date
        )
        # print(result["orders"]["data"]["orders"], "welcome>")
        easycom_orders = result["orders"]["data"]["orders"]

        mapped_orders = []

        for request in easycom_orders:
            suborders = request.get("suborders", [])
            products = []

            for product in suborders:
                products.append(
                    {
                        "name": safe(product.get("productName"), "Unknown Product"),
                        "sku_code": safe(product.get("sku"), "NO-SKU"),
                        "quantity": parse_numeric(product.get("item_quantity"), 1),
                        "unit_price": parse_numeric(
                            product.get("mrp") or product.get("selling_price"), 0
                        ),
                    }
                )

            # Calculate order value
            order_value = sum(
                parse_numeric(p.get("item_quantity"), 1)
                * parse_numeric(p.get("mrp") or p.get("selling_price"), 0)
                for p in suborders
            )

            body = {
                # Consignee details
                "consignee_full_name": safe(request.get("customer_name"), "No Name"),
                "consignee_phone": safe(request.get("contact_num"), "9999999999"),
                "consignee_email": safe(request.get("email"), "noemail@example.com"),
                "consignee_address": safe(
                    request.get("address_line_1"), "Address Missing"
                ),
                "consignee_landmark": safe(
                    request.get("address_line_2"), "Landmark Missing"
                ),
                "consignee_pincode": safe(request.get("pin_code"), "000000"),
                "consignee_city": safe(request.get("city"), "Unknown City"),
                "consignee_state": safe(request.get("state"), "Unknown State"),
                "consignee_country": "India",
                # Order details
                "order_id": str(
                    safe(request.get("reference_code"), f"ORDER-{uuid.uuid4().hex[:6]}")
                ),
                "order_date": parse_datetime_safe(request.get("order_date")),
                "channel": "easyecom",
                "order_type": "B2C",
                "billing_is_same_as_consignee": True,
                "products": products,
                "payment_mode": (
                    "prepaid" if request.get("payment_mode_id") == 5 else "COD"
                ),
                "total_amount": parse_numeric(request.get("total_amount")),
                "order_value": order_value,
                "client_id": 2,
                "company_id": 1,
                "source": "easyecom",
                "marketplace_order_id": str(safe(request.get("invoice_id"), "0")),
                "status": "new",
                "sub_status": "new",
                # Package dimensions
                "length": parse_numeric(request.get("Package Length")),
                "breadth": parse_numeric(request.get("Package Width")),
                "height": parse_numeric(request.get("Package Height")),
                "weight": parse_numeric(request.get("Package Weight")),
                "volumetric_weight": parse_numeric(request.get("volumetric_weight")),
                "applicable_weight": parse_numeric(request.get("applicable_weight")),
                # Charges
                "discount": parse_numeric(request.get("discount")),
                "tax_amount": parse_numeric(request.get("total_tax")),
                "shipping_charges": parse_numeric(
                    request.get("total_shipping_charge"), 0
                ),
                "cod_charges": parse_numeric(request.get("cod_charges"), 0),
                "gift_wrap_charges": parse_numeric(request.get("gift_wrap_charges"), 0),
                "other_charges": parse_numeric(request.get("other_charges"), 0),
                # Additional info
                "tracking_info": [],
                "action_history": [],
                "invoice_number": str(safe(request.get("invoice_number"), "")),
                "eway_bill_number": str(safe(request.get("eway_bill_number"), "")),
                "pickup_location_code": "0005",  # str(safe(request.get("location_key"), "005")),ur61332503716
                "zone": "",
                "product_quantity": parse_numeric(request.get("order_quantity"), 1),
            }

            mapped_orders.append(body)

        print(mapped_orders[0]["marketplace_order_id"], "<mapped_orders>")
        async with get_db_session() as db:  # use async with
            # Fetch order
            result = await db.execute(
                select(Order).filter_by(order_id=str(body["order_id"]), client_id=2)
            )
            new_order = result.scalars().first()

            # mapped_orders[0]["marketplace_order_id"]

            if not new_order:
                print("welcome to new order")
                new_order = Order.create_db_entity(body)
                db.add(new_order)
                await db.commit()
                await db.refresh(new_order)  # ensures all fields are updated from DB
                print("Last inserted ID:", new_order.id)  # access the primary key

                # Fetch order info (if needed)
                # NOTE = I HAVE  ADDED `invoice_id` INVOICE ID STATIC YOU CAN CHANGE ACCORDING TO YOUR REQUIREMENT
                # result = await OrderService.get_easyecom_order_details(
                #     invoice_id=str(mapped_orders[0]["marketplace_order_id"]),
                #     api_token=api_token,
                # )
                # print(result, "GET ORDER INFO")

                # ADD CARRIER CREDENTIALS  THIS IS OPTIONAL YOU CAN REMOVE IF NOT NEEDED BECAUSE ON MY ACCOUNT I HAVE ADDED CARRIER CERDENTIALS
                # result = await OrderService.add_carrier_credentials(
                #     api_token="c04b200bec738367faf360cce33a4e078f1cdce399cf7efc64da1899983e8fbd",
                #     carrier_id=5859, #THIS IS STATICALLY USER NAME I HAVE GIVEN
                #     username="dummy", #THIS IS STATICALLY USER NAME I HAVE GIVEN
                #     password="dummy", #THIS IS STATICALLY USER NAME I HAVE GIVEN
                #     token=api_token
                # )

                # WHEN I HIT THIS I GET RESPONSE AS BELOW
                # {
                # "code": 200,
                # "message": "Successfully added the Courier with carrier_id 5859",
                # "data": {
                # "webhookToken": "NPzKOzOW3J4U_247654_5859",
                # "webhookUrl": "https://webhook.easyecom.io/webhook/UpdateTrackingStatus?carrier_token=NPzKOzOW3J4U_247654_5859",
                # "companyCarrierId": 174519
                # }
                # }

                # result = await OrderService.assign_awb(
                #     api_token=api_token,
                #     invoice_id=mapped_orders[0]["marketplace_order_id"], #I HAVE ENTER THIS STATICALY YOU CAN CHANGE ACCORDING REG.
                #     courier="Handover", #THIS IS COURIER NAME WHICH HAVE SHIP THE ORDER
                #     awb_num="89078766788", #THIS IS AWB NUMBER WHICH COMES FROM COURIER
                #     company_carrier_id=174519, #THIS IS STATIC ID WHICH COMES WHEN YOU ADD CARRIER CREDENTIALS `companyCarrierId`
                #     shipping_label_path="path/to/shipping_label.pdf", # Path to your shipping label PDF
                #     invoice_url="https://s3-us-west-2.amazonaws.com/ee-uploaded-files-oregon/NewMarketplaceInvoice/8/62838396.pdf?request-content-type=application/force-download", #THIS IS INVOICE URL WHICH COMES FROM COURIER API RESPONSE
                #     origin_code="65489", #THIS IS ORIGI N CODE I HAVE ENTERED IT STATICALY
                #     destination_code="165489" #THIS IS DESTINATION I HAVE ENTERED IT STATICALY
                # )

                # print(result)

            else:
                print("Order already exists", new_order.order_id)
        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="synced successfully",
        )

    @staticmethod
    def dev_cancel_awbs():

        time.sleep(3)
        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="cancelled successfully",
        )

    @staticmethod
    async def get_previous_orders(
        order_id: str, page_number: int = 1, batch_size: int = 10
    ):
        """
        Get previous orders for the same phone number as the given order ID with pagination (ASYNC VERSION)
        """
        try:
            async with get_db_session() as db:
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # -----------------------------------------------------------
                # 1. Get Current Order (we need phone number)
                # -----------------------------------------------------------
                current_order_stmt = select(Order).where(
                    Order.order_id == order_id,
                    Order.company_id == company_id,
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                )

                result = await db.execute(current_order_stmt)
                current_order = result.scalars().first()

                if not current_order:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        message="Order not found",
                        status=False,
                    )

                if not current_order.consignee_phone:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order does not have a phone number",
                        status=False,
                    )

                phone_number = current_order.consignee_phone

                # -----------------------------------------------------------
                # 2. Build base query for previous orders
                # -----------------------------------------------------------
                base_query = (
                    select(Order)
                    .options(joinedload(Order.pickup_location))
                    .where(
                        Order.consignee_phone == phone_number,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        Order.order_id != order_id,
                    )
                )

                # -----------------------------------------------------------
                # 3. Status counts (group by)
                # -----------------------------------------------------------
                status_count_stmt = (
                    select(Order.status, func.count(Order.id))
                    .where(
                        Order.consignee_phone == phone_number,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        Order.order_id != order_id,
                    )
                    .group_by(Order.status)
                )

                status_result = await db.execute(status_count_stmt)
                status_rows = status_result.all()

                status_counts = {status: count for status, count in status_rows}

                # Grouping
                rto_count = status_counts.get("rto", 0) + status_counts.get(
                    "rto_delivered", 0
                )
                delivered_count = status_counts.get("delivered", 0)
                other_count = sum(
                    cnt
                    for st, cnt in status_counts.items()
                    if st not in ["rto", "rto_delivered", "delivered"]
                )

                categorized_status_counts = {
                    "rto": rto_count,
                    "delivered": delivered_count,
                    "others": other_count,
                    "total": sum(status_counts.values()),
                }

                # -----------------------------------------------------------
                # 4. Total count
                # -----------------------------------------------------------
                count_stmt = select(func.count(Order.id)).where(
                    Order.consignee_phone == phone_number,
                    Order.company_id == company_id,
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                    Order.order_id != order_id,
                )

                count_result = await db.execute(count_stmt)
                total_count = count_result.scalar() or 0

                # -----------------------------------------------------------
                # 5. Pagination
                # -----------------------------------------------------------
                offset_value = (page_number - 1) * batch_size

                data_stmt = (
                    base_query.order_by(
                        desc(Order.order_date),
                        desc(Order.created_at),
                        desc(Order.id),
                    )
                    .offset(offset_value)
                    .limit(batch_size)
                )

                data_result = await db.execute(data_stmt)
                previous_orders = data_result.scalars().all()

                # -----------------------------------------------------------
                # 6. Convert orders to response models
                # -----------------------------------------------------------
                previous_orders_response = [
                    Order_Response_Model(**order.to_model().model_dump())
                    for order in previous_orders
                ]

                # Pagination info
                total_pages = (total_count + batch_size - 1) // batch_size
                has_next = page_number < total_pages
                has_prev = page_number > 1

                # -----------------------------------------------------------
                # 7. Final Response
                # -----------------------------------------------------------
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Previous orders fetched successfully",
                    data={
                        "current_order_id": order_id,
                        "phone_number": phone_number,
                        "previous_orders": previous_orders_response,
                        "status_counts": categorized_status_counts,
                        "pagination": {
                            "current_page": page_number,
                            "batch_size": batch_size,
                            "total_count": total_count,
                            "total_pages": total_pages,
                            "has_next": has_next,
                            "has_prev": has_prev,
                        },
                    },
                    status=True,
                )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error fetching previous orders: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database error occurred while fetching previous orders.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Internal server error occurred.",
            )

    @staticmethod
    def get_customers(phone: str):

        try:

            print("inside")

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            print(phone)

            # query the db for fetching the orders

            customers = (
                db.query(Order)
                .filter(
                    Order.client_id == client_id,
                    Order.consignee_phone == phone,
                )
                .distinct(Order.consignee_address)
                .all()
            )

            print(customers)

            # applying company and client filter

            customer_data = [
                customerResponseModel(
                    **order.to_model().model_dump(),
                )
                for order in customers
            ]

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                data={"customers": customer_data},
                status=True,
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fetching Customers: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetchin the customers.",
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

        finally:
            if db:
                db.close()

    @staticmethod
    async def export_orders(order_filters: Order_filters):
        try:
            # destructure the filters
            order_status = order_filters.order_status
            search_term = order_filters.search_term
            start_date = order_filters.start_date
            end_date = order_filters.end_date
            payment_mode = order_filters.payment_mode
            courier_filter = order_filters.courier_filter
            sku_codes = order_filters.sku_codes
            order_id = order_filters.order_id

            # ✅ FIX 1: Convert timezone-aware to naive
            if start_date and start_date.tzinfo is not None:
                start_date = start_date.replace(tzinfo=None)

            if end_date and end_date.tzinfo is not None:
                end_date = end_date.replace(tzinfo=None)

            db: AsyncSession = get_db_session()

            client_id = context_user_data.get().client_id

            # BASE QUERY
            stmt = (
                select(Order)
                .options(joinedload(Order.pickup_location))
                .where(
                    and_(
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        cast(Order.order_date, DateTime) >= start_date,
                        cast(Order.order_date, DateTime) <= end_date,
                    )
                )
            )

            # Search Term Logic
            if search_term:
                search_terms = [t.strip() for t in search_term.split(",")]

                conditions = []
                for term in search_terms:
                    conditions.append(
                        or_(
                            Order.order_id == term,
                            Order.awb_number == term,
                            Order.consignee_phone == term,
                            Order.consignee_alternate_phone == term,
                            Order.consignee_email == term,
                        )
                    )

                stmt = stmt.where(or_(*conditions))

            # SKU filter
            if sku_codes:
                sku_list = [s.strip() for s in sku_codes.split(",")]
                like_conditions = [
                    cast(Order.products, String).like(f'%"sku_code": "{sku}"%')
                    for sku in sku_list
                ]
                stmt = stmt.where(or_(*like_conditions))

            # Payment mode
            if payment_mode:
                stmt = stmt.where(Order.payment_mode == payment_mode)

            # Courier filter
            if courier_filter:
                stmt = stmt.where(Order.courier_partner == courier_filter)

            # Status filter
            if order_status != "all":
                stmt = stmt.where(Order.status == order_status)

            # Order ID filter
            if order_id:
                order_ids = [o.strip() for o in order_id.split(",")]
                stmt = stmt.where(Order.order_id.in_(order_ids))

            # Ordering
            stmt = stmt.order_by(desc(Order.order_date), desc(Order.created_at))

            # FETCH DATA
            result = await db.execute(stmt)
            fetched_orders = result.scalars().all()

            orders_data = []

            for order in fetched_orders:
                body = {
                    "Order ID": order.order_id,
                    "Order Date": (
                        order.order_date.strftime("%Y-%m-%d")
                        if order.order_date
                        else ""
                    ),
                    "Channel": order.channel,
                    "Consignee Full Name": order.consignee_full_name,
                    "Consignee Phone": order.consignee_phone,
                    "Consignee Alternate Phone": order.consignee_alternate_phone,
                    "Consignee Email": order.consignee_email,
                    "Consignee Company": order.consignee_company,
                    "Consignee GSTIN": order.consignee_gstin,
                    "Consignee Address": order.consignee_address,
                    "Consignee Landmark": order.consignee_landmark,
                    "Consignee Pincode": order.consignee_pincode,
                    "Consignee City": order.consignee_city,
                    "Consignee State": order.consignee_state,
                    "Consignee Country": order.consignee_country,
                    "Billing is Same as Consignee": order.billing_is_same_as_consignee,
                    "Billing Full Name": order.billing_full_name,
                    "Billing Phone": order.billing_phone,
                    "Billing Email": order.billing_email,
                    "Billing Address": order.billing_address,
                    "Billing Landmark": order.billing_landmark,
                    "Billing Pincode": order.billing_pincode,
                    "Billing City": order.billing_city,
                    "Billing State": order.billing_state,
                    "Billing Country": order.billing_country,
                    "Pickup Location Details": {
                        "location_type": (
                            order.pickup_location.location_type
                            if order.pickup_location
                            else ""
                        ),
                        "alternate_phone": (
                            order.pickup_location.alternate_phone
                            if order.pickup_location
                            else ""
                        ),
                        "address": (
                            order.pickup_location.address
                            if order.pickup_location
                            else ""
                        ),
                        "location_code": (
                            order.pickup_location.location_code
                            if order.pickup_location
                            else ""
                        ),
                        "contact_person_name": (
                            order.pickup_location.contact_person_name
                            if order.pickup_location
                            else ""
                        ),
                        "pincode": (
                            order.pickup_location.pincode
                            if order.pickup_location
                            else ""
                        ),
                    },
                    "Payment Mode": order.payment_mode,
                    "Total Amount": order.total_amount,
                    "Order Value": order.order_value,
                    "Shipping Charges": order.shipping_charges,
                    "COD Charges": order.cod_charges,
                    "Discount": order.discount,
                    "Gift Wrap Charges": order.gift_wrap_charges,
                    "Other Charges": order.other_charges,
                    "Tax Amount": order.tax_amount,
                    "Eway Bill Number": order.eway_bill_number,
                    "Length": order.length,
                    "Breadth": order.breadth,
                    "Height": order.height,
                    "Weight": order.weight,
                    "Applicable Weight": order.applicable_weight,
                    "Volumetric Weight": order.volumetric_weight,
                    "Courier Partner": order.courier_partner,
                    "AWB Number": order.awb_number,
                    "Status": order.sub_status,
                    "delivered_date": (
                        order.delivered_date.strftime("%Y-%m-%d")
                        if order.delivered_date
                        else ""
                    ),
                    "booking_date": (
                        order.booking_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.booking_date
                        else ""
                    ),
                    "edd": order.edd.strftime("%Y-%m-%d") if order.edd else "",
                    "pickup_completion_date": (
                        order.pickup_completion_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.pickup_completion_date
                        else ""
                    ),
                    "First Out for Pickup Date": (
                        order.first_ofp_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.first_ofp_date
                        else ""
                    ),
                    "Pickup failure reason": order.pickup_failed_reason or "",
                    "First Out for Delivery Date": (
                        order.first_ofd_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.first_ofd_date
                        else ""
                    ),
                    "RTO Initiated Date": (
                        order.rto_initiated_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.rto_initiated_date
                        else ""
                    ),
                    "RTO Delivered Date": (
                        order.rto_delivered_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.rto_delivered_date
                        else ""
                    ),
                    "RTO Reason": order.rto_reason or "",
                    "Forward Freight": order.forward_freight or 0,
                    "Forward COD Charge": order.forward_cod_charge or 0,
                    "Forward Tax": order.forward_tax or 0,
                    "RTO Freight": order.rto_freight or 0,
                    "RTO Tax": order.rto_tax or 0,
                }

                orders_data.append(body)

            # Create dataframe
            df = pd.DataFrame(orders_data)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Orders")

            output.seek(0)

            return base64.b64encode(output.getvalue()).decode("utf-8")

        except Exception as e:
            logger.error(msg=f"Unhandled error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def temp_export_orders(order_filters: Order_filters):

        try:

            db = get_db_session()

            orders = (
                db.query(Order)
                .filter(
                    Order.status != "new",
                    Order.status != "cancelled",
                    # Order.client_id == 71,
                )
                .all()
            )

            fetched_orders = []
            count = 0
            for order in orders:
                try:

                    # Extract the datetime from the first object
                    booking_datetime = (
                        order.booking_date if order.booking_date else order.order_date
                    )
                    if not booking_datetime:
                        continue

                    # booking_datetime = order.order_date

                    # Parse the datetime string into a datetime object

                    # Check if the booking date is in December 2024
                    if booking_datetime.year == 2025 and (booking_datetime.month == 3):

                        if order.forward_freight is None:

                            print(order.order_id)

                            contract = (
                                db.query(Company_To_Client_Contract)
                                .join(Company_To_Client_Contract.aggregator_courier)
                                .filter(
                                    Company_To_Client_Contract.client_id
                                    == order.client_id,
                                    Aggregator_Courier.slug == order.courier_partner,
                                )
                                .options(
                                    joinedload(
                                        Company_To_Client_Contract.aggregator_courier
                                    )
                                )
                                .first()
                            )

                            client_id = order.client_id

                            context_user_data.set(TempModel(**{"client_id": client_id}))

                            if contract is None:

                                contract = (
                                    db.query(Company_To_Client_Contract)
                                    .join(Company_To_Client_Contract.aggregator_courier)
                                    .filter(
                                        Company_To_Client_Contract.client_id
                                        == order.client_id
                                    )
                                    .options(
                                        joinedload(
                                            Company_To_Client_Contract.aggregator_courier
                                        )
                                    )
                                    .first()
                                )

                            freight = ServiceabilityService.calculate_freight(
                                order_id=order.order_id,
                                min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                                additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                                contract_id=contract.id,
                            )

                            order.forward_freight = freight["freight"]
                            order.forward_cod_charge = freight["cod_charges"]
                            order.forward_tax = freight["tax_amount"]

                        if order.status == "RTO" and order.rto_freight is None:

                            try:

                                contract = (
                                    db.query(Company_To_Client_Contract)
                                    .join(Company_To_Client_Contract.aggregator_courier)
                                    .filter(
                                        Company_To_Client_Contract.client_id
                                        == order.client_id,
                                        Aggregator_Courier.slug
                                        == order.courier_partner,
                                    )
                                    .options(
                                        joinedload(
                                            Company_To_Client_Contract.aggregator_courier
                                        )
                                    )
                                    .first()
                                )
                            except:
                                contract = (
                                    db.query(Company_To_Client_Contract)
                                    .join(Company_To_Client_Contract.aggregator_courier)
                                    .filter(
                                        Company_To_Client_Contract.client_id
                                        == order.client_id
                                    )
                                    .options(
                                        joinedload(
                                            Company_To_Client_Contract.aggregator_courier
                                        )
                                    )
                                    .first()
                                )

                            client_id = order.client_id

                            context_user_data.set(TempModel(**{"client_id": client_id}))

                            try:
                                rto_freight = ServiceabilityService.calculate_rto_freight(
                                    order_id=order.order_id,
                                    min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                                    additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                                    contract_id=contract.id,
                                )
                            except:
                                continue

                            order.rto_freight = rto_freight["rto_freight"]
                            order.rto_tax = rto_freight["rto_tax"]
                            order.forward_cod_charge = 0
                            order.forward_tax = float(order.forward_freight) * 0.18

                        fetched_orders.append(order)
                        print(count)
                        count += 1

                except (json.JSONDecodeError, IndexError, ValueError) as e:
                    # Handle malformed JSON or missing keys
                    print(f"Error while processing tracking_info: {e}")
                    continue

            # fetch the orders in descending order of order date
            # query = query.order_by(desc(Order.order_date), desc(Order.created_at))

            # fetched_orders = query.all()

            # for order in fetched_orders:

            orders_data = [
                {
                    "Order ID": order.order_id,
                    "Order Date": (
                        order.order_date.strftime("%Y-%m-%d")
                        if order.order_date
                        else ""
                    ),
                    "client_id": order.client_id,
                    "Channel": order.channel,
                    "Consignee Full Name": order.consignee_full_name,
                    "Consignee Phone": order.consignee_phone,
                    "Consignee Alternate Phone": order.consignee_alternate_phone,
                    "Consignee Email": order.consignee_email,
                    "Consignee Company": order.consignee_company,
                    "Consignee GSTIN": order.consignee_gstin,
                    "Consignee Address": order.consignee_address,
                    "Consignee Landmark": order.consignee_landmark,
                    "Consignee Pincode": order.consignee_pincode,
                    "Consignee City": order.consignee_city,
                    "Consignee State": order.consignee_state,
                    "Consignee Country": order.consignee_country,
                    "Payment Mode": order.payment_mode,
                    "Total Amount": order.total_amount,
                    "Order Value": order.order_value,
                    "Shipping Charges": order.shipping_charges,
                    "COD Charges": order.cod_charges,
                    "Discount": order.discount,
                    "Gift Wrap Charges": order.gift_wrap_charges,
                    "Other Charges": order.other_charges,
                    "Tax Amount": order.tax_amount,
                    "Eway Bill Number": order.eway_bill_number,
                    "Length": order.length,
                    "Breadth": order.breadth,
                    "Height": order.height,
                    "Weight": order.weight,
                    "Applicable Weight": order.applicable_weight,
                    "Volumetric Weight": order.volumetric_weight,
                    "Courier Partner": order.courier_partner,
                    "AWB Number": order.awb_number,
                    "Status": order.sub_status,
                    "forward_freight": order.forward_freight,
                    "forward_cod_charge": order.forward_cod_charge,
                    "forward_tax": order.forward_tax,
                    "rto_freight": order.rto_freight,
                    "rto_tax": order.rto_tax,
                    "delivered_date": (
                        order.delivered_date.strftime("%Y-%m-%d")
                        if order.delivered_date
                        else ""
                    ),
                    "booking_date": (
                        order.booking_date.strftime("%Y-%m-%d %H:%M:%S")
                        if order.booking_date
                        else ""
                    ),
                    "edd": order.edd.strftime("%Y-%m-%d") if order.edd else "",
                    "zone": order.zone,
                }
                for order in fetched_orders
            ]

            # Create a DataFrame
            df = pd.DataFrame(orders_data)

            # Create an in-memory bytes buffer
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Orders")

            # Return the file as a downloadable response
            output.seek(0)
            headers = {
                "Content-Disposition": 'attachment; filename="orders.xlsx"',
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }
            return base64.b64encode(output.getvalue()).decode("utf-8")

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
    def get_remittance():

        try:

            with get_db_session() as db:

                client_id = context_user_data.get().client_id

                today = datetime.utcnow()

                query = db.query(COD_Remittance)

                # applying company and client filter
                cycles = (
                    query.filter(
                        COD_Remittance.client_id == client_id,
                    )
                    .order_by(desc(COD_Remittance.payout_date))
                    .all()
                )

                remittance_cycles = [cycle.to_model().model_dump() for cycle in cycles]

                # Total COD
                total_cod_generated = (
                    db.query(func.sum(COD_Remittance.generated_cod))
                    .filter(COD_Remittance.client_id == client_id)
                    .scalar()
                    or 0
                )

                # Total COD
                total_cod_paid = (
                    db.query(
                        (
                            func.sum(COD_Remittance.generated_cod)
                            - func.sum(COD_Remittance.freight_deduction)
                            - func.sum(COD_Remittance.early_cod_charges)
                        )
                    )
                    .filter(
                        COD_Remittance.client_id == client_id,
                        COD_Remittance.status == "paid",
                    )
                    .scalar()
                    or 0
                )

                # Future COD
                future_cod = (
                    db.query(
                        func.sum(
                            COD_Remittance.generated_cod
                            - COD_Remittance.freight_deduction
                            - COD_Remittance.early_cod_charges
                        )
                    )
                    .filter(
                        COD_Remittance.client_id == client_id,
                        COD_Remittance.status == "pending",
                    )
                    .scalar()
                    or 0
                )

                # Next COD (generated_cod of next upcoming cycle)
                next_cod = (
                    db.query(
                        COD_Remittance.generated_cod
                        - COD_Remittance.freight_deduction
                        - COD_Remittance.early_cod_charges
                    )
                    .filter(
                        COD_Remittance.client_id == client_id,
                        COD_Remittance.status == "pending",
                    )
                    .order_by(COD_Remittance.payout_date.asc())
                    .limit(1)
                    .scalar()
                    or 0
                )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Orders fetched Successfully",
                    data={
                        "remittance_cycles": remittance_cycles,
                        "summary": {
                            "total_cod_generated": total_cod_generated,
                            "future_cod": future_cod,
                            "total_cod_paid": total_cod_paid,
                            "next_cod": next_cod,
                        },
                    },
                    status=True,
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
    def get_remittance_orders(cycle_id: int):

        try:

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # query the db for fetching the orders

            query = db.query(Order)

            # # applying company and client filter
            fetched_orders = query.filter(
                Order.company_id == company_id,
                Order.client_id == client_id,
                Order.cod_remittance_cycle_id == cycle_id,
            ).all()

            fetched_orders = [
                Order_Response_Model(
                    **order.to_model().model_dump(),
                )
                for order in fetched_orders
            ]

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                data={"orders": fetched_orders},
                status=True,
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

        finally:
            if db:
                db.close()

    @staticmethod
    async def get_order_by_Id(order_id: str):
        try:
            db: AsyncSession = get_db_session()
            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id
            # Build async query
            query = (
                select(Order)
                .where(
                    Order.company_id == company_id,
                    Order.client_id == client_id,
                    Order.order_id == order_id,
                    Order.is_deleted == False,
                )
                .options(joinedload(Order.pickup_location))
            )
            result = await db.execute(query)
            order_data = result.scalars().first()
            if not order_data:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Order not found",
                    status=False,
                )
            # Convert to response
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Order fetched successfully",
                data=Single_Order_Response_Model(**order_data.to_model().model_dump()),
                status=True,
            )
        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching Order: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the Order.",
            )
        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )
        finally:
            # ALWAYS close async DB connection
            if db:
                await db.close()

    @staticmethod
    def convert_order_data(order):

        couriers = {
            "Delhivery": "delhivery",
            "Xpressbees": "xpressbees",
            "Ekart": "ekart",
            "Bluedart": "bluedart",
            "Bluedart-air": "bluedart-air",
            "DTDC": "dtdc",
        }

        # Parse products and package details from strings to Python objects
        parsed_products = json.loads(order.get("products", "[]"))
        parsed_package_details = json.loads(order.get("package_details", "[]"))

        order_value, total_amount = calculate_order_values(order)

        order_status = order.get("status", "")
        default_mapping = {
            "status": "new",
            "sub_status": "new",
        }  # Default values

        status_info = status_mapping.get(order_status, default_mapping)

        # Assigning status and sub_status
        final_status = status_info["status"]
        final_sub_status = status_info["sub_status"]

        try:
            body = {
                "length": (
                    float(parsed_package_details[0].get("length", 0))
                    if parsed_package_details
                    else 0
                ),
                "breadth": (
                    float(parsed_package_details[0].get("breadth", 0))
                    if parsed_package_details
                    else 0
                ),
                "height": (
                    float(parsed_package_details[0].get("height", 0))
                    if parsed_package_details
                    else 0
                ),
                "weight": (
                    float(parsed_package_details[0].get("weight", 0))
                    if parsed_package_details
                    else 0
                ),
                "payment_mode": (
                    "prepaid" if order.get("payment_mode", "") == "prepaid" else "COD"
                ),
                "shipping_charges": order.get("shipping_charges", 0),
                "cod_charges": order.get("cod_charges", 0),
                "discount": order.get("discount", 0),
                "gift_wrap_charges": 0,  # Assuming this value isn't in the original object
                "other_charges": 0,  # Assuming this value isn't in the original object
                "total_amount": total_amount,
                "order_value": order_value,
                "tax_amount": order.get("tax_amount", 0),
                "pickup_location_code": "0002",
                "billing_is_same_as_consignee": True,  # Assuming it's always the same
                "billing_full_name": "",
                "billing_phone": "",
                "billing_email": "",
                "billing_address": "",
                "billing_landmark": "",  # Assuming this value isn't in the original object
                "billing_pincode": "",
                "billing_city": "",
                "billing_state": "",
                "billing_country": "India",
                "consignee_full_name": order.get("consignee_name", ""),
                "consignee_phone": order.get("consignee_phone", ""),
                "consignee_email": order.get("consignee_email", ""),
                "consignee_alternate_phone": "",  # Assuming this value isn't in the original object
                "consignee_company": (
                    order.get("consignee_company", "")
                    if order.get("consignee_company", "")
                    else ""
                ),
                "consignee_gstin": "",  # Assuming this value isn't in the original object
                "consignee_address": order.get("consignee_address", ""),
                "consignee_landmark": "",  # Assuming this value isn't in the original object
                "consignee_pincode": (str(order.get("billing_pincode", ""))),
                "consignee_city": (
                    order.get("consignee_city", "")
                    if order.get("consignee_city", "")
                    else ""
                ),
                "consignee_state": (
                    order.get("consignee_state", "")
                    if order.get("consignee_state", "")
                    else ""
                ),
                "consignee_country": "India",
                "order_id": str(order.get("order_id", "")),
                "order_date": datetime.strptime(
                    order.get("order_date", ""), "%Y-%m-%d %H:%M:%S"
                ).isoformat(),
                "channel": (
                    order.get("channel", "custom")
                    if order.get("channel", "custom")
                    else "custom"
                ),
                "status": final_status,
                "sub_status": final_sub_status,
                "aggregator": (
                    "shiperfecto"
                    if order.get("courier_partner") == "shipperfecto"
                    else ""
                ),
                "courier_partner": couriers.get(order.get("delivery_partner", ""), ""),
                "awb_number": order.get("awb_number", ""),
                "shipment_mode": "surface",
                "products": [
                    {
                        "name": product.get("name", ""),
                        "unit_price": float(product.get("unit_price", 0)),
                        "quantity": int(product.get("quantity", 0)),
                        "sku_code": product.get("sku_code", ""),
                    }
                    for product in parsed_products
                ],
            }

            return body

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response

            return None

    @staticmethod
    def order_put(order_data):
        try:

            with get_db_session() as db:

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                order_data["order_id"] = str(order_data["order_id"])

                order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_data["order_id"],
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                    )
                    .first()
                )

                # Throw an error if an order id for that client already exists
                if order:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        data={"order_id": order_data["order_id"]},
                        message="Order Id already exists",
                    )

                order_data = OrderService.convert_order_data(order_data)

                if "consignee_phone" in order_data and str(
                    order_data["consignee_phone"]
                ).startswith("+91"):
                    order_data["consignee_phone"] = str(order_data["consignee_phone"])[
                        3:
                    ]  # Remove the "+91"

                if "billing_phone" in order_data and str(
                    order_data["billing_phone"]
                ).startswith("+91"):
                    order_data["billing_phone"] = str(order_data["billing_phone"])[
                        3:
                    ]  # Remove the "+91"

                # Add company and client id to the order

                print(3)

                order_data["client_id"] = client_id
                order_data["company_id"] = company_id

                # adding the extra default details to the order

                order_data["order_type"] = "B2C"

                # round the volumetric weight to 3 decimal places
                volumetric_weight = round(
                    (
                        order_data["length"]
                        * order_data["breadth"]
                        * order_data["height"]
                    )
                    / 5000,
                    3,
                )

                applicable_weight = round(
                    max(order_data["weight"], volumetric_weight), 3
                )

                order_data["applicable_weight"] = applicable_weight
                order_data["volumetric_weight"] = volumetric_weight

                print(4)

                if order_data["consignee_pincode"]:
                    print(order_data["consignee_pincode"])
                    pincode_data = (
                        db.query(Pincode_Mapping)
                        .filter(
                            Pincode_Mapping.pincode
                            == int(order_data["consignee_pincode"])
                        )
                        .first()
                    )

                    order_data["consignee_city"] = (
                        order_data.get("consignee_city", "")
                        if order_data.get("consignee_city", "")
                        else pincode_data.city
                    )
                    order_data["consignee_state"] = (
                        order_data.get("consignee_state", "")
                        if order_data.get("consignee_state", "")
                        else pincode_data.state
                    )

                print(4.5)

                # fetch the pickup location pincode
                pickup_pincode: int = (
                    db.query(Pickup_Location.pincode)
                    .filter(
                        Pickup_Location.location_code
                        == order_data["pickup_location_code"]
                    )
                    .first()
                )[0]

                print(5)

                if pickup_pincode is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pickup Location",
                    )

                # calculating shipping zone for the order
                zone_data = ShipmentService.calculate_shipping_zone(
                    pickup_pincode, order_data["consignee_pincode"]
                )

                print(6)

                # return error message if could not calculate zone
                if not zone_data.status:
                    GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pincodes",
                        status=False,
                    )

                zone = zone_data.data.get("zone", "")
                order_data["zone"] = zone

                print(7)

                order_model_instance = Order.create_db_entity(order_data)

                created_order = Order.create_new_order(order_model_instance)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order created Successfully",
                    data={"order_id": created_order.order_id},
                    status=True,
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
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
                message="Unhandled error: {}".format(str(e)),
            )

    @classmethod
    def order_from_marketplace(this, request: object):
        try:

            with get_db_session() as db:

                status = request["status"]

                if status == "checkout-draft":
                    return

                if status == "failed":
                    return

                if status == "cancelled":
                    return

                if request["com_id"] == "heyansh":
                    pickup_location_code = "0001"

                elif request["com_id"] == "ahanamall":
                    pickup_location_code = "0003"

                newOrder = {
                    {
                        "consignee_full_name": request["billing"]["first_name"]
                        + " "
                        + request["billing"]["last_name"],
                        "consignee_phone": request["billing"]["phone"],
                        "consignee_email": request["billing"]["email"],
                        "consignee_alternate_phone": "",
                        "consignee_company": request["billing"]["company"],
                        "consignee_gstin": "",
                        "consignee_address": request["billing"]["address_1"]
                        + " "
                        + request["billing"]["address_2"],
                        "consignee_landmark": "",
                        "consignee_pincode": request["billing"]["postcode"],
                        "consignee_city": request["billing"]["city"],
                        "consignee_state": request["billing"]["state"],
                        "consignee_country": "India",
                        "billing_is_same_as_consignee": True,
                        "billing_full_name": "",
                        "billing_phone": "",
                        "billing_email": "",
                        "billing_address": "",
                        "billing_landmark": "",
                        "billing_pincode": "",
                        "billing_city": "",
                        "billing_state": "",
                        "billing_country": "India",
                        "pickup_location_code": pickup_location_code,
                        "order_id": str(request["id"]),
                        "order_date": request["date_created"].replace("T", " "),
                        "channel": "woocommerce",
                        "products": [
                            {
                                "name": product["name"],
                                "quantity": str(product["quantity"]),
                                "unit_price": str(product["price"]),
                                "sku_code": str(product["sku"]),
                            }
                            for product in request["line_items"]
                        ],
                        "payment_mode": (
                            "COD" if request["payment_method"] == "cod" else "prepaid"
                        ),
                        "shipping_charges": 0,
                        "cod_charges": 0,
                        "discount": 0,
                        "gift_wrap_charges": 0,
                        "tax_percentage": 0,
                        "other_charges": 0,
                        "eway_bill_number": "",
                        "weight": 0.49,
                        "length": 10,
                        "breadth": 10,
                        "height": 10,
                        "tax_amount": request["total_tax"],
                        "total_amount": (
                            request["total"]
                            if request["prices_include_tax"] == True
                            else float(request["total"]) + float(request["total_tax"])
                        ),
                        "order_value": (
                            request["total"]
                            if request["prices_include_tax"] == True
                            else float(request["total"]) + float(request["total_tax"])
                        ),
                    }
                }

                existing_order = (
                    db.query(Order)
                    .filter(
                        Order.company_id == str(request["com_id"]),
                        Order.order_id == str(request["id"]),
                    )
                    .first()
                )

                if existing_order:
                    # If the order exists, update its attributes
                    for key, value in newOrder.items():
                        setattr(existing_order, key, value)

                    # Update the tracking info
                    existing_order.tracking_info.append(
                        {
                            "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
                            "event": "Order Updated on Platform",
                        }
                    )

                    # Commit the changes to the database
                    db.commit()
                    return {
                        "code": 200,
                        "message": "Order Updated",
                    }  # Return the updated order
                else:
                    # If the order does not exist, create a new one

                    final = Order(**{**newOrder, "status": "New"})
                    final.tracking_info = [
                        {
                            "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
                            "event": "Order Created on Platform",
                        }
                    ]

                    db.add(final)
                    db.commit()

                return {"code": 200, "message": "Order Created"}

        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": "Error",
            }

        except Exception as e:
            # Handle any other exceptions, including unexpected ones
            logger.error("An unexpected error occurred: %s", e)
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred. Please try again later.",
            )

    @classmethod
    def update_Dimentions(file: List[dict]):
        try:
            with get_db_session() as db:
                if file is not None:
                    print(file, "|*|<<file>>|*|")
                else:
                    raise ValueError("No file was uploaded.")
        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": "Error",
            }
        except Exception as e:
            # Handle any other exceptions, including unexpected ones
            logger.error("An unexpected error occurred: %s", e)
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred. Please try again later.",
            )

    @staticmethod
    def bulk_update_Dimensions(bulkDimensionsUpdate: BulkDimensionUpdateModel):
        try:
            with get_db_session() as db:
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                orders_input_map = {
                    str(d.order_id).strip(): d
                    for d in bulkDimensionsUpdate.bulk_dimensions
                }
                order_ids = list(orders_input_map.keys())

                # Fetch all matching orders in one query
                existing_orders = (
                    db.query(Order)
                    .filter(
                        Order.order_id.in_(order_ids),
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.status == "new",
                    )
                    .all()
                )

                # Map from order_id to Order object
                existing_order_map = {
                    order.order_id: order for order in existing_orders
                }

                # Identify missing orders
                missing_orders = list(set(order_ids) - set(existing_order_map.keys()))

                # Apply bulk updates
                for order_id, order in existing_order_map.items():
                    order_data = orders_input_map[order_id]

                    volumetric_weight = round(
                        (order_data.length * order_data.breadth * order_data.height)
                        / 5000,
                        3,
                    )
                    applicable_weight = round(
                        max(order_data.dead_weight, volumetric_weight), 3
                    )

                    order.length = order_data.length
                    order.breadth = order_data.breadth
                    order.height = order_data.height
                    order.applicable_weight = applicable_weight
                    order.volumetric_weight = volumetric_weight
                    order.weight = order_data.dead_weight

                    db.add(order)

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message=f"Updated {len(existing_order_map)} out of {len(bulkDimensionsUpdate.bulk_dimensions)} orders",
                    status=True,
                )

        except DatabaseError as e:
            logger.error(f"Database error during bulk update: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database error occurred while updating dimensions",
                status=False,
            )

        except Exception as e:
            logger.error("Unexpected error in bulk_update_Dimensions: %s", str(e))
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred. Please try again later.",
                status=False,
            )

    @staticmethod
    def update_pickup_location(
        update_pickup_payload: UpdatePickupLocationModel,
    ) -> GenericResponseModel:
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            with get_db_session() as db:

                location = (
                    db.query(Pickup_Location)
                    .filter(
                        Pickup_Location.client_id == client_id,
                        Pickup_Location.location_code
                        == update_pickup_payload.location_code,
                    )
                    .first()
                )

                # throw error if not client location is not found
                if location is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        message="Invalid location",
                    )

                db.query(Order).filter(
                    Order.order_id.in_(update_pickup_payload.order_ids)
                ).update(
                    {Order.pickup_location_code: location.location_code},
                    synchronize_session=False,
                )

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.CREATED,
                    status=True,
                    message="Default Location updated successfully",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Could not update default location: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update default location",
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
    def get_bulk_upload_logs():
        """
        Get bulk upload logs for a client with pagination
        """
        try:
            db = get_db_session()
            client_id = context_user_data.get().client_id

            print("im insidet he best thing")

            query = (
                db.query(BulkOrderUploadLogs)
                .filter(BulkOrderUploadLogs.client_id == client_id)
                .order_by(BulkOrderUploadLogs.upload_date.desc())
            )

            total_count = query.count()

            print("Total count of bulk upload logs:", total_count)

            logs = query.limit(10).all()

            logs_data = [log.to_model() for log in logs]

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Bulk upload logs retrieved successfully",
                status=True,
                data={
                    "logs": logs_data,
                },
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching bulk upload logs: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error fetching bulk upload logs",
                status=False,
            )

    @staticmethod
    def generate_validation_error_excel(
        validation_errors: List[dict],
        filename: str = "bulk_upload_validation_errors.xlsx",
    ) -> str:
        """
        Generate an Excel file with validation errors in the exact format of the upload template
        Users can fix errors directly in this file and re-upload

        Args:
            validation_errors: List of dictionaries containing order data with validation errors
            filename: Name of the output Excel file

        Returns:
            Base64 encoded Excel file content
        """
        try:
            logger.info(
                extra=context_user_data.get(),
                msg=f"Generating validation error Excel file with {len(validation_errors)} error records",
            )

            # Use the ErrorExcelGenerator to create the file
            excel_base64 = ErrorExcelGenerator.generate_error_excel(
                validation_errors, filename
            )

            logger.info(
                extra=context_user_data.get(),
                msg=f"Successfully generated validation error Excel file: {filename}",
            )

            return excel_base64

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error generating validation error Excel file: {str(e)}",
            )
            raise e

    @staticmethod
    def format_validation_error_for_excel(
        order_data: dict, validation_errors: List[str], error_messages: dict
    ) -> dict:
        """
        Format order data and validation errors for Excel generation

        Args:
            order_data: Original order data from bulk upload
            validation_errors: List of field names with validation errors
            error_messages: Dictionary mapping field names to error messages

        Returns:
            Formatted dictionary ready for Excel generation
        """
        try:
            # Create a copy of the original order data
            formatted_data = order_data.copy()

            # Add error information
            formatted_data["error_fields"] = validation_errors

            # Create error description
            error_descriptions = []
            suggested_fixes = []

            for field in validation_errors:
                if field in error_messages:
                    error_descriptions.append(f"{field}: {error_messages[field]}")

                    # Add specific suggestions based on field type
                    if field in [
                        "consignee_phone",
                        "consignee_alternate_phone",
                        "billing_phone",
                    ]:
                        suggested_fixes.append("Use 10-digit phone number")
                    elif field in ["consignee_email", "billing_email"]:
                        suggested_fixes.append(
                            "Use valid email format (user@domain.com)"
                        )
                    elif field in ["consignee_pincode", "billing_pincode"]:
                        suggested_fixes.append("Use 6-digit pincode")
                    elif field == "payment_mode":
                        suggested_fixes.append("Use 'COD' or 'Prepaid'")
                    elif field == "order_date":
                        suggested_fixes.append("Use YYYY-MM-DD format")
                    elif field in ["unit_price", "shipping_charges", "cod_charges"]:
                        suggested_fixes.append("Use numeric values only")
                    elif field in ["length", "breadth", "height", "weight"]:
                        suggested_fixes.append("Use numeric values greater than 0")
                    else:
                        suggested_fixes.append("Fill required field")

            formatted_data["error_description"] = "; ".join(error_descriptions)
            formatted_data["error_field"] = ", ".join(validation_errors)
            formatted_data["suggested_fix"] = "; ".join(
                list(set(suggested_fixes))
            )  # Remove duplicates

            return formatted_data

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error formatting validation error data: {str(e)}",
            )
            raise e

    @staticmethod
    def validate_bulk_upload_data(
        orders_data: List[dict],
    ) -> tuple[List[dict], List[dict]]:
        """
        Validate bulk upload data and separate valid orders from invalid ones

        Args:
            orders_data: List of order dictionaries from bulk upload

        Returns:
            Tuple of (valid_orders, invalid_orders_with_errors)
        """
        try:
            valid_orders = []
            invalid_orders = []

            # Frontend validation mapping
            required_fields = {
                "order_id": "Order ID",
                "order_date": "Order Date",
                "consignee_full_name": "Consignee Full Name",
                "consignee_phone": "Consignee Phone Number",
                "consignee_email": "Consignee Email",
                "consignee_address": "Shipping Address",
                "consignee_pincode": "Shipping Pincode",
                "consignee_city": "Shipping City",
                "consignee_state": "Shipping State",
                "consignee_country": "Shipping Country",
                "billing_is_same_as_consignee": "Is Billing Same as Shipping",
                "pickup_location_code": "Pickup Location Code",
                "name": "Product Name",
                "unit_price": "Product Unit Price",
                "quantity": "Product Quantity",
                "length": "Package Length",
                "breadth": "Package Breadth",
                "height": "Package Height",
                "weight": "Package Weight",
                "payment_mode": "Payment Mode",
            }

            for order_data in orders_data:
                validation_errors = []
                error_messages = {}

                # Check required fields
                for field_key, field_name in required_fields.items():
                    if (
                        not order_data.get(field_key)
                        or str(order_data.get(field_key)).strip() == ""
                    ):
                        validation_errors.append(field_key)
                        error_messages[field_key] = f"{field_name} is required"

                # Validate phone numbers (10 digits)
                phone_fields = [
                    "consignee_phone",
                    "consignee_alternate_phone",
                    "billing_phone",
                ]
                for phone_field in phone_fields:
                    phone_value = order_data.get(phone_field, "")
                    if phone_value and not re.match(
                        r"^\d{10}$", str(phone_value).strip()
                    ):
                        validation_errors.append(phone_field)
                        error_messages[phone_field] = "Phone number must be 10 digits"

                # Validate email format
                email_fields = ["consignee_email", "billing_email"]
                for email_field in email_fields:
                    email_value = order_data.get(email_field, "")
                    if email_value and not re.match(
                        r"^[^@]+@[^@]+\.[^@]+$", str(email_value).strip()
                    ):
                        validation_errors.append(email_field)
                        error_messages[email_field] = "Invalid email format"

                # Validate pincode (6 digits)
                pincode_fields = ["consignee_pincode", "billing_pincode"]
                for pincode_field in pincode_fields:
                    pincode_value = order_data.get(pincode_field, "")
                    if pincode_value and not re.match(
                        r"^\d{6}$", str(pincode_value).strip()
                    ):
                        validation_errors.append(pincode_field)
                        error_messages[pincode_field] = "Pincode must be 6 digits"

                # Validate payment mode
                payment_mode = order_data.get("payment_mode", "")
                if payment_mode and str(payment_mode).strip().upper() not in [
                    "COD",
                    "PREPAID",
                ]:
                    validation_errors.append("payment_mode")
                    error_messages["payment_mode"] = (
                        "Payment mode must be 'COD' or 'Prepaid'"
                    )

                # Validate numeric fields
                numeric_fields = [
                    "unit_price",
                    "quantity",
                    "length",
                    "breadth",
                    "height",
                    "weight",
                ]
                for numeric_field in numeric_fields:
                    value = order_data.get(numeric_field, "")
                    if value:
                        try:
                            float_value = float(str(value).strip())
                            if float_value <= 0:
                                validation_errors.append(numeric_field)
                                error_messages[numeric_field] = (
                                    f"{numeric_field} must be greater than 0"
                                )
                        except (ValueError, TypeError):
                            validation_errors.append(numeric_field)
                            error_messages[numeric_field] = (
                                f"{numeric_field} must be a valid number"
                            )

                # Validate date format
                order_date = order_data.get("order_date", "")
                if order_date:
                    try:
                        datetime.strptime(str(order_date).strip(), "%Y-%m-%d")
                    except ValueError:
                        validation_errors.append("order_date")
                        error_messages["order_date"] = (
                            "Order date must be in YYYY-MM-DD format"
                        )

                # Categorize order
                if validation_errors:
                    formatted_error_data = (
                        OrderService.format_validation_error_for_excel(
                            order_data, validation_errors, error_messages
                        )
                    )
                    invalid_orders.append(formatted_error_data)
                else:
                    valid_orders.append(order_data)

            logger.info(
                extra=context_user_data.get(),
                msg=f"Bulk upload validation completed: {len(valid_orders)} valid, {len(invalid_orders)} invalid",
            )

            return valid_orders, invalid_orders

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error validating bulk upload data: {str(e)}",
            )
            raise e

    @staticmethod
    def terminate_idle_database_connections():
        """
        Adhoc function to terminate idle database connections.
        Executes: SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE state = 'idle' AND pid <> pg_backend_pid()
        """
        try:
            db = get_db_session()

            # Execute the query to terminate idle connections
            result = db.execute(
                text(
                    """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE state = 'idle' 
                AND pid <> pg_backend_pid()
            """
                )
            )

            terminated_count = 0
            for row in result:
                if row[0]:  # pg_terminate_backend returns true if successful
                    terminated_count += 1

            logger.info(
                extra=context_user_data.get(),
                msg=f"Terminated {terminated_count} idle database connections",
            )

            return {
                "status": "success",
                "terminated_connections": terminated_count,
                "message": f"Successfully terminated {terminated_count} idle database connections",
            }

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error terminating idle database connections: {str(e)}",
            )
            return {
                "status": "error",
                "message": f"Failed to terminate idle connections: {str(e)}",
            }

    @staticmethod
    def calculate_zone_optimized(source_pincode_record, destination_pincode_record):
        """
        Optimized zone calculation function that works with already fetched pincode records
        """
        try:
            if not source_pincode_record or not destination_pincode_record:
                return "D"

            # For A Zone -> Same city
            if (
                source_pincode_record.city.lower()
                == destination_pincode_record.city.lower()
            ):
                return "A"

            # For B Zone -> Same state
            if (
                source_pincode_record.state.lower()
                == destination_pincode_record.state.lower()
            ):
                return "B"

            # For E Zone -> Special Zones
            if (
                source_pincode_record.state.lower() in special_zone
                or destination_pincode_record.state.lower() in special_zone
            ):
                return "E"

            # For C Zone -> Metro to Metro
            if source_pincode_record.city.lower() in [
                city.lower() for city in metro_cities
            ] and destination_pincode_record.city.lower() in [
                city.lower() for city in metro_cities
            ]:
                return "C"

            # Default to D Zone
            return "D"

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error calculating zone: {str(e)}",
            )
            return "D"
