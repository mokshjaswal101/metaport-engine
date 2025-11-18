import http
from uuid import uuid4

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
    def create_order(
        order_data: Order_create_request_model,
    ):

        try:
            print(order_data, "before process")

            courier_id = order_data.courier
            del order_data.courier

            db = get_db_session()

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
                            status=True,
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
                    shipmentResponse = ShipmentService.assign_awb(
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

            order_data = order_data.model_dump()

            # Add company and client id to the order

            order_data["client_id"] = client_id
            order_data["company_id"] = company_id

            # adding the extra default details to the order

            order_data["order_type"] = "B2C"

            # rounde the volumetric weight to 3 decimal places
            volumetric_weight = round(
                (order_data["length"] * order_data["breadth"] * order_data["height"])
                / 5000,
                3,
            )

            applicable_weight = round(max(order_data["weight"], volumetric_weight), 3)

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

            # fetch the pickup location pincode
            pickup_pincode: int = (
                db.query(Pickup_Location.pincode)
                .filter(
                    Pickup_Location.location_code == order_data["pickup_location_code"],
                    Pickup_Location.client_id == client_id,
                )
                .first()
            )[0]

            if pickup_pincode is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pickup Location",
                )

            blocked_pickup_pincodes = [
                "110018",
                "110031",
                "110041",
                "110033",
                "110043",
                "110018",
            ]

            if client_id == 93 and pickup_pincode in blocked_pickup_pincodes:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Pickup location not servicable",
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

            created_order = Order.create_new_order(order_model_instance)

            db.commit()
            print(courier_id, "**courier_id**")
            if courier_id is not None:
                shipmentResponse = ShipmentService.assign_awb(
                    CreateShipmentModel(
                        order_id=order_data["order_id"],
                        contract_id=courier_id,
                    )
                )
                print("inside shipment response", shipmentResponse)
                db.commit()
                return shipmentResponse
            else:
                # logger.info(">START COURIER WITH COURIER PRIORITY>")
                # START COURIER PRIORITY FEATURE
                result_config_setting = (
                    db.query(
                        Courier_Priority_Config_Setting.courier_method,
                    )
                    .filter(
                        Courier_Priority_Config_Setting.client_id == client_id,
                        Courier_Priority_Config_Setting.company_id == company_id,
                        Courier_Priority_Config_Setting.status == True,
                    )
                    .first()
                )
                if result_config_setting:
                    courier_method = result_config_setting[0]
                    print(courier_method, "<<courier_method>>")
                    if courier_method == "courier_assign_rules":
                        # logger.info(
                        #     f"I am {courier_method} Enabled for this order_id => {order_data["order_id"]}"
                        # )
                        courier_priority_rules = (
                            db.query(Courier_Priority_Rules)
                            .filter(
                                and_(
                                    Courier_Priority_Rules.status == True,
                                    Courier_Priority_Rules.client_id == client_id,
                                )
                            )
                            .order_by(Courier_Priority_Rules.ordering_key.asc())
                            .all()
                        )

                        if (
                            len(courier_priority_rules) > 0
                            and courier_priority_rules != None
                        ):
                            print("I AM UNDER APPLU RULES FETCH DATA")
                            list_Section = ShipmentService.apply_rules_and_fetch_data(
                                courier_priority_rules,
                                order_data["order_id"],
                                db,
                            )
                            print(list_Section, "|>*|list_Section|<*|")
                            if len(list_Section) > 0:
                                return list_Section[0]
                            else:
                                print("NO Order Shipped With Courier Assing ")

                            # logger.info(
                            #     f"There are No Rule Available for this order_id => {order_data["order_id"]}"
                            # )

                    if courier_method == "courier_priority":
                        # logger.info(
                        #      f"I am {courier_method} Enabled for this order_id => {order_data["order_id"]}"
                        # )
                        courier_priority = (
                            db.query(Courier_Priority.priority_type)
                            .filter(Courier_Priority.client_id == client_id)
                            .first()
                        )
                        # logger.info(
                        #     f"Courier Priority NEW ACTION {format(str(courier_priority))}"
                        # )
                        if courier_priority != None:
                            assign_priority_wise_courier = (
                                ShipmentService.assign_priority_wise_courier(
                                    courier_priority[0], order_data["order_id"]
                                )
                            )
                            print(
                                jsonable_encoder(assign_priority_wise_courier),
                                ">**FINAL ACTION TRIGGER**< I AM ORDER SERVICE FILE",
                            )
                            if (
                                len(assign_priority_wise_courier) > 0
                                and assign_priority_wise_courier != None
                            ):
                                print(
                                    "Successfull assign AWB=>",
                                    assign_priority_wise_courier,
                                )
                                return assign_priority_wise_courier[0]
                            else:
                                # Return error response
                                return GenericResponseModel(
                                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                                    message="An error occurred while creating the Order.",
                                )
                        else:
                            # logger.info(
                            #     f"There are No Courier Priority Selected order_id => {order_data["order_id"]}"
                            # )
                            return GenericResponseModel(
                                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                                message="An error occurred while creating the Order.",
                            )
                else:
                    # logger.info(">>** Courier-Allocation Feature IS DISABLED **<<")
                    print(">>** Courier-Allocation Feature IS DISABLED **<<")

            # IF CLIENT HAS ADDED CUSTOM PERIORITY

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
    def update_order(order_id: str, order_data: Order_create_request_model):

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
                        data={"order_id": order_data.order_id},
                        message="Order does not exist",
                    )

                if order.status != "new":
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order cannot be updated",
                    )

                # check if the new order id is already present or not

                if order_id != order_data.order_id:

                    check_existing_order = (
                        db.query(Order)
                        .filter(
                            Order.order_id == order_data.order_id,
                            Order.company_id == company_id,
                            Order.client_id == client_id,
                        )
                        .first()
                    )

                    # if order not found, throw an error
                    if check_existing_order:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            data={"order_id": order_data.order_id},
                            message="Order id cannot be updates as order id already exists",
                        )

                order_data = order_data.model_dump()

                # Process and update all the fields in the order with the new data
                for key, value in order_data.items():
                    setattr(order, key, value)

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

                order.applicable_weight = applicable_weight
                order.volumetric_weight = volumetric_weight

                # Convert to UTC
                order.order_date = convert_to_utc(order_date=order.order_date)

                new_activity = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Order Updated on Platform",
                    "user_data": context_user_data.get().id,
                }
                order.action_history.append(new_activity)

                # fetch the pickup location pincode
                pickup_pincode: int = (
                    db.query(Pickup_Location.pincode)
                    .filter(
                        Pickup_Location.location_code
                        == order_data["pickup_location_code"]
                    )
                    .first()
                )[0]

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

                zone = zone_data.data.get("zone", "E")
                order.zone = zone

                order.sub_status = "new"

                # Commit the updated order to the database
                db.add(order)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order updated Successfully",
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
    def delete_order(order_id: str):

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
                        data={"order_id": order.order_id},
                        message="Order does not exist",
                    )

                if order.status != "new" and order.status != "cancelled":
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Order cannot be deleted",
                    )

                order.is_deleted = True

                db.add(order)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order updated Successfully",
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
    def clone_order(order_id: str):
        try:

            db = get_db_session()

            # ids = [
            #     "3422",
            #     "3453",
            #     "3502",
            #     "3545",
            #     "3429",
            #     "3470",
            #     "3505",
            #     "3408",
            #     "3455",
            #     "3484",
            #     "3419",
            #     "3503",
            #     "3468",
            #     "3497",
            #     "3432",
            #     "3463",
            #     "3500",
            #     "3434",
            #     "3489",
            #     "3516",
            #     "3421",
            #     "3465",
            #     "3493",
            #     "3414",
            #     "3488",
            #     "3427",
            #     "3464",
            #     "3499",
            #     "3402",
            #     "3457",
            #     "3487",
            #     "3411",
            #     "3469",
            #     "3492",
            #     "3410",
            #     "3451",
            #     "3481",
            #     "3437",
            #     "3476",
            #     "3436",
            #     "3483",
            #     "3531",
            #     "3405",
            #     "3458",
            #     "3486",
            #     "3472",
            #     "3398",
            #     "3449",
            #     "3480",
            #     "3417",
            #     "3508",
            #     "3400",
            #     "3435",
            #     "3482",
            #     "3529",
            #     "3439",
            #     "3498",
            #     "3532",
            #     "3446",
            #     "3490",
            #     "3528",
            #     "3404",
            #     "3452",
            #     "3471",
            #     "3425",
            #     "3474",
            #     "3510",
            #     "3416",
            #     "3456",
            #     "3494",
            #     "3424",
            #     "3475",
            #     "3506",
            #     "3403",
            #     "3454",
            #     "3485",
            #     "3413",
            #     "3466",
            #     "3513",
            #     "3442",
            #     "3445",
            #     "3479",
            #     "3459",
            #     "3473",
            #     "3507",
            #     "3509",
            #     "3530",
            #     "3578",
            #     "3443",
            #     "3501",
            #     "3514",
            #     "3431",
            #     "3478",
            #     "3433",
            #     "3495",
            #     "3396",
            #     "3511",
            #     "3552",
            #     "3394",
            #     "3448",
            #     "3477",
            #     "3496",
            #     "3418",
            #     "3491",
            #     "3519",
            #     "3428",
            #     "3462",
            #     "3440",
            #     "3515",
            #     "3564",
            # ]

            # # COD Remittance Validation Logic
            # print("Starting COD Remittance validation...")

            # # Get all COD remittance records for the specified IDs
            # cod_remittances = (
            #     db.query(COD_Remittance).filter(COD_Remittance.id.in_(ids)).all()
            # )

            # mismatched_ids = []

            # for cod_remittance in cod_remittances:
            #     # Get all orders linked to this COD remittance cycle
            #     orders_in_cycle = (
            #         db.query(Order)
            #         .filter(Order.cod_remittance_cycle_id == cod_remittance.id)
            #         .all()
            #     )

            #     # Calculate actual totals from orders
            #     actual_order_count = len(orders_in_cycle)
            #     actual_total_amount = sum(
            #         float(order.total_amount or 0) for order in orders_in_cycle
            #     )

            #     # Get COD remittance recorded values
            #     recorded_order_count = cod_remittance.order_count or 0
            #     recorded_generated_cod = float(cod_remittance.generated_cod or 0)

            #     # Check for mismatches
            #     amount_mismatch = (
            #         abs(actual_total_amount - recorded_generated_cod) > 0.01
            #     )  # Using small tolerance for decimal comparison
            #     count_mismatch = actual_order_count != recorded_order_count

            #     if amount_mismatch or count_mismatch:
            #         mismatch_info = {
            #             "cod_remittance_id": cod_remittance.id,
            #             "amount_mismatch": amount_mismatch,
            #             "count_mismatch": count_mismatch,
            #             "actual_order_count": actual_order_count,
            #             "recorded_order_count": recorded_order_count,
            #             "actual_total_amount": actual_total_amount,
            #             "recorded_generated_cod": recorded_generated_cod,
            #         }
            #         mismatched_ids.append(mismatch_info)

            #         print(f"MISMATCH FOUND for COD Remittance ID: {cod_remittance.id}")
            #         if amount_mismatch:
            #             print(
            #                 f"  Amount Mismatch - Actual: {actual_total_amount}, Recorded: {recorded_generated_cod}"
            #             )
            #         if count_mismatch:
            #             print(
            #                 f"  Count Mismatch - Actual: {actual_order_count}, Recorded: {recorded_order_count}"
            #             )
            #     else:
            #         print(
            #             f"✓ COD Remittance ID {cod_remittance.id} - No mismatch found"
            #         )

            # # Print summary of mismatched IDs
            # if mismatched_ids:
            #     print(
            #         f"\nSUMMARY: Found {len(mismatched_ids)} COD remittance records with mismatches:"
            #     )
            #     for mismatch in mismatched_ids:
            #         print(f"ID: {mismatch['cod_remittance_id']}")
            # else:
            #     print(
            #         "\n✓ All COD remittance records validated successfully - no mismatches found!"
            #     )

            # # Return validation results instead of continuing with clone logic
            # return GenericResponseModel(
            #     status_code=http.HTTPStatus.OK,
            #     message="COD Remittance validation completed",
            #     status=True,
            #     data={
            #         "total_validated": len(cod_remittances),
            #         "mismatched_count": len(mismatched_ids),
            #         "mismatched_ids": [m["cod_remittance_id"] for m in mismatched_ids],
            #         "detailed_mismatches": mismatched_ids,
            #     },
            # )

            # # Import required modules for calculation
            # from data.courier_buy_rates import calculate_buy_freight
            # from models.courier_billing import CourierBilling
            # from decimal import Decimal
            # from sqlalchemy.orm import joinedload

            # # Fetch orders with their billing records in a single query to avoid N+1
            # orders = (
            #     db.query(Order)
            #     .filter(
            #         Order.aggregator == "xpressbees",
            #         Order.booking_date > "2025-06-01 00:00:00.000 +0530",
            #         Order.courier_partner.in_(["xpressbees", "xpressbees 1kg"]),
            #     )
            #     .options(joinedload(Order.courier_billing))
            #     .order_by(desc(Order.booking_date))
            #     .all()
            # )

            # size = len(orders)
            # count = 1

            # for order in orders:

            #     print("processing ", count, " of ", size, " orders")
            #     count += 1
            #     # calculate the shiperfecto freight using shiperfecto rates
            #     # and then assign to courier_billing calculated_freight and calculated_tax

            #     try:
            #         # Get billing records from the eagerly loaded relationship
            #         billing_records = [
            #             record
            #             for record in order.courier_billing
            #             if not record.is_deleted
            #         ]

            #         create_new_record = False
            #         if not billing_records:
            #             print(
            #                 f"No billing records found for order {order.order_id}, will create new record"
            #             )
            #             create_new_record = True

            #         # Prepare parameters for rate calculation
            #         from data.courier_buy_rates import normalize_courier_partner

            #         aggregator = "xpressbees"
            #         raw_courier_partner = (
            #             order.courier_partner if order.courier_partner else ""
            #         )

            #         # Normalize courier partner name to match configuration
            #         courier_partner = normalize_courier_partner(
            #             aggregator, raw_courier_partner
            #         )

            #         if not courier_partner:
            #             print(
            #                 f"  Could not determine courier partner for order {order.order_id}: {raw_courier_partner}"
            #             )
            #             continue

            #         zone = order.zone.lower() if order.zone else "d"
            #         applicable_weight = float(order.applicable_weight)
            #         order_value = float(order.total_amount)
            #         payment_mode = (
            #             order.payment_mode.lower() if order.payment_mode else "cod"
            #         )

            #         # Check if order is RTO
            #         is_rto = (
            #             order.courier_status and "rto" in order.courier_status.lower()
            #         )

            #         print(f"Calculating for Order ID: {order.order_id}")
            #         print(
            #             f"  Raw Courier: {raw_courier_partner} -> Normalized: {courier_partner}"
            #         )
            #         print(
            #             f"  Zone: {zone}, Weight: {applicable_weight}, Value: {order_value}"
            #         )
            #         print(f"  Payment: {payment_mode}, RTO: {is_rto}")

            #         # Calculate buy freight using the courier_buy_rates module
            #         freight_calculation = calculate_buy_freight(
            #             aggregator=aggregator,
            #             courier=courier_partner,
            #             zone=zone,
            #             applicable_weight=applicable_weight,
            #             order_value=order_value,
            #             payment_mode=payment_mode,
            #             is_rto=is_rto,
            #         )

            #         if "error" in freight_calculation:
            #             print(
            #                 f"  Error calculating freight: {freight_calculation['error']}"
            #             )
            #             continue

            #         # Extract calculated values
            #         calculated_freight = Decimal(str(freight_calculation["freight"]))
            #         calculated_tax = Decimal(str(freight_calculation["tax_amount"]))

            #         print(
            #             f"  Calculated - Freight: {calculated_freight}, Tax: {calculated_tax}"
            #         )

            #         if create_new_record:
            #             # Create new courier billing record
            #             new_billing_record = CourierBilling(
            #                 order_id=order.id,
            #                 awb_number=order.awb_number,
            #                 calculated_freight=calculated_freight,
            #                 calculated_tax=calculated_tax,
            #                 final_freight=None,
            #                 final_tax=None,
            #             )
            #             db.add(new_billing_record)
            #             # db.flush()  # Get the ID
            #             print(
            #                 f"  Created new billing record ID: {new_billing_record.id}"
            #             )
            #         else:
            #             # Update existing billing records for this order
            #             for billing_record in billing_records:
            #                 if (
            #                     billing_record.calculated_freight is None
            #                     or billing_record.calculated_tax is None
            #                 ):
            #                     billing_record.calculated_freight = calculated_freight
            #                     billing_record.calculated_tax = calculated_tax
            #                     print(
            #                         f"  Updated billing record ID: {billing_record.id}"
            #                     )

            #         # Commit after each order to avoid large transactions

            #     except Exception as e:
            #         print(f"Error processing order {order.order_id}: {str(e)}")
            #         db.rollback()
            #         continue

            #     db.commit()

            # return

            # adhoc

            # import os
            # import pandas as pd
            # from openpyxl import Workbook

            # pincodes = [
            #     "302026",
            #     "303103",
            #     "401105",
            #     "395004",
            #     "395010",
            #     "421302",
            # ]

            # for pincode in pincodes:

            #     # Read the CSV file
            #     csv_file_path = os.path.join(
            #         os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            #         f"{pincode}.csv",
            #     )

            #     if os.path.exists(csv_file_path):
            #         try:
            #             # Read CSV file
            #             df = pd.read_csv(csv_file_path)

            #             # Filter to only consider data with courier = "blue dart"
            #             if "Courier" in df.columns:
            #                 df = df[df["Courier"].str.lower() == "blue dart"]

            #             # Check if required columns exist
            #             if "Delivery Pincode" in df.columns:
            #                 source_pincode = pincode

            #                 # Create zone mapping based on delivery pincodes
            #                 zone_mapping = []

            #                 # Get source pincode mapping record for zone calculation
            #                 source_pincode_record = (
            #                     db.query(Pincode_Mapping)
            #                     .filter(Pincode_Mapping.pincode == int(source_pincode))
            #                     .first()
            #                 )

            #                 # Extract all delivery pincodes from DataFrame
            #                 all_delivery_pincodes = [
            #                     int(row["Delivery Pincode"])
            #                     for index, row in df.iterrows()
            #                 ]

            #                 # Fetch all pincode mapping records in a single query to avoid N+1 problem
            #                 pincode_mapping_records = (
            #                     db.query(Pincode_Mapping)
            #                     .filter(
            #                         Pincode_Mapping.pincode.in_(all_delivery_pincodes)
            #                     )
            #                     .all()
            #                 )

            #                 # Create a dictionary for quick lookup
            #                 pincode_mapping_dict = {
            #                     record.pincode: record
            #                     for record in pincode_mapping_records
            #                 }

            #                 for index, row in df.iterrows():
            #                     delivery_pincode = str(row["Delivery Pincode"])

            #                     print(delivery_pincode, ">>delivery_pincode<<")

            #                     print(
            #                         len(pincode_mapping_records),
            #                         ">>pincode_mapping_records<<",
            #                     )

            #                     # service_prepaid = str(row["ekart_lm_prepaid"])
            #                     # service_cod = str(row["ekart_lm_cod"])

            #                     # print(
            #                     #     service_cod,
            #                     #     service_prepaid,
            #                     #     ">>service_cod, service_prepaid<<",
            #                     # )

            #                     # if service_prepaid != "True" and service_cod != "True":
            #                     #     continue

            #                     # Get destination pincode mapping record from pre-fetched dictionary
            #                     destination_pincode_record = pincode_mapping_dict.get(
            #                         int(delivery_pincode)
            #                     )

            #                     # Initialize zone_calculation_method variable
            #                     zone_calculation_method = "Fallback"

            #                     if destination_pincode_record:
            #                         print(
            #                             destination_pincode_record.pincode,
            #                             ">>destination_pincode_record<<",
            #                         )

            #                     # Use serviceability zone calculation logic
            #                     if source_pincode_record and destination_pincode_record:
            #                         calculated_zone = (
            #                             OrderService.calculate_zone_optimized(
            #                                 source_pincode_record,
            #                                 destination_pincode_record,
            #                             )
            #                         )
            #                         zone_calculation_method = "Serviceability Logic"

            #                     else:
            #                         # Fallback to simple calculation if pincode mapping not found
            #                         calculated_zone = "D"
            #                         zone_calculation_method = "Fallback"

            #                     print(calculated_zone)

            #                     zone_mapping.append(
            #                         {
            #                             "Source_Pincode": source_pincode,
            #                             "Delivery_Pincode": delivery_pincode,
            #                             "Courier": "Bluedart",
            #                             "Zone": calculated_zone,
            #                         }
            #                     )

            #                 # Create DataFrame from zone mapping
            #                 zone_df = pd.DataFrame(zone_mapping)

            #                 # Save to Excel file
            #                 excel_file_path = os.path.join(
            #                     os.path.dirname(
            #                         os.path.dirname(os.path.dirname(__file__))
            #                     ),
            #                     f"zone_mapping_{pincode}.xlsx",
            #                 )

            #                 with pd.ExcelWriter(
            #                     excel_file_path, engine="openpyxl"
            #                 ) as writer:
            #                     zone_df.to_excel(
            #                         writer, sheet_name="Zone_Mapping", index=False
            #                     )

            #                     # Add summary sheet
            #                     # summary_data = zone_df.groupby("Calculated_Zone").agg(
            #                     #     {"Delivery_Pincode": "count"}
            #                     # )
            #                     # summary_data.columns = ["Count"]
            #                     # summary_data.to_excel(writer, sheet_name="Zone_Summary")

            #                     # # Add method breakdown summary
            #                     # method_summary = (
            #                     #     zone_df.groupby(
            #                     #         ["Zone_Calculation_Method", "Calculated_Zone"]
            #                     #     )
            #                     #     .size()
            #                     #     .unstack(fill_value=0)
            #                     # )
            #                     # method_summary.to_excel(
            #                     #     writer, sheet_name="Method_Summary"
            #                     # )

            #                 logger.info(
            #                     f"Zone mapping created successfully. Total records: {len(zone_mapping)}"
            #                 )
            #                 logger.info(f"Excel file saved at: {excel_file_path}")

            #                 # return GenericResponseModel(
            #                 #     status_code=http.HTTPStatus.OK,
            #                 #     message=f"Zone mapping created successfully. {len(zone_mapping)} records processed and saved to Excel.",
            #                 #     status=True,
            #                 #     data={
            #                 #         "total_records": len(zone_mapping),
            #                 #         "excel_file_path": excel_file_path,
            #                 #         "zone_distribution": zone_df["Calculated_Zone"]
            #                 #         .value_counts()
            #                 #         .to_dict(),
            #                 #     },
            #                 # )
            #             else:
            #                 # return GenericResponseModel(
            #                 #     status_code=http.HTTPStatus.BAD_REQUEST,
            #                 #     message="'Delivery Pincode' column not found in CSV file",
            #                 #     status=False,
            #                 # )
            #                 continue

            #         except Exception as csv_error:
            #             logger.error(f"Error processing CSV file: {str(csv_error)}")
            #             return GenericResponseModel(
            #                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            #                 message=f"Error processing CSV file: {str(csv_error)}",
            #                 status=False,
            #             )
            #     else:
            #         return GenericResponseModel(
            #             status_code=http.HTTPStatus.NOT_FOUND,
            #             message="CSV file RTO.csv not found in root directory",
            #             status=False,
            #         )

            # return

            # client_id = 93

            # orders = (
            #     db.query(Order)
            #     .filter(
            #         Order.client_id == client_id,
            #         Order.status != "new",
            #         Order.status != "cancelled",
            #         Order.tracking_info != None,
            #         Order.aggregator.in_(["shiperfecto"]),
            #         Order.booking_date > "2025-10-15 23:42:09.430 +0530",
            #     )
            #     .all()
            # )

            # # print(orders)

            # # return

            # for order in orders:

            #     try:

            #         tracking_info = order.tracking_info

            #         if tracking_info is None or len(tracking_info) == 0:
            #             continue

            #         body = {
            #             "awb": order.awb_number,
            #             "current_status": order.sub_status,
            #             "order_id": order.order_id,
            #             "current_timestamp": (
            #                 order.tracking_info[0]["datetime"]
            #                 if order.tracking_info
            #                 else order.booking_date.strftime("%d-%m-%Y %H:%M:%S")
            #             ),
            #             "shipment_status": order.sub_status,
            #             "scans": [
            #                 {
            #                     "datetime": activity["datetime"],
            #                     "status": activity["status"],
            #                     "location": activity["location"],
            #                 }
            #                 for activity in order.tracking_info
            #             ],
            #         }

            #         response = requests.post(
            #             url="https://wtpzsmej1h.execute-api.ap-south-1.amazonaws.com/prod/webhook/bluedart",
            #             verify=True,
            #             timeout=10,
            #             json=body,
            #         )

            #         print(response.json())
            #         print(order.order_id)

            #     except:
            #         continue

            # return

            # file_path = "1.csv"
            # OrderService.bulk_upload_courier_billing_from_file(file_path)

            # file_path = "3.csv"
            # OrderService.bulk_upload_courier_billing_from_file(file_path)

            # file_path = "1.csv"
            # OrderService.bulk_upload_courier_billing_from_file(file_path)

            # file_path = "2.csv"
            # return OrderService.bulk_upload_courier_billing_from_file(file_path)

            # # COD Remittance validation logic - Optimized to avoid N+1 queries
            # cod_remittance_mismatches = []

            # # Single query to get all COD remittances with their calculated order totals
            # remittance_totals_query = (
            #     db.query(
            #         COD_Remittance.id.label("remittance_id"),
            #         COD_Remittance.client_id,
            #         COD_Remittance.generated_cod,
            #         COD_Remittance.payout_date,
            #         COD_Remittance.status,
            #         func.coalesce(func.sum(Order.total_amount), 0).label(
            #             "calculated_total"
            #         ),
            #         func.count(Order.id).label("order_count"),
            #     )
            #     .filter(COD_Remittance.client_id != 108, COD_Remittance.client_id != 26)
            #     .outerjoin(Order, Order.cod_remittance_cycle_id == COD_Remittance.id)
            #     .group_by(
            #         COD_Remittance.id,
            #         COD_Remittance.client_id,
            #         COD_Remittance.generated_cod,
            #         COD_Remittance.payout_date,
            #         COD_Remittance.status,
            #     )
            #     .all()
            # )

            # logger.info(
            #     f"Found {len(remittance_totals_query)} COD remittance entries to validate"
            # )

            # # Process results and identify mismatches
            # mismatched_remittance_ids = []
            # count = 0

            # for result in remittance_totals_query:
            #     count += 1
            #     print(f"checking cycle - {count}")

            #     cod_generated = float(result.generated_cod)
            #     calculated_total = float(result.calculated_total)

            #     if (
            #         abs(calculated_total - cod_generated) > 0.01
            #     ):  # Allow for small floating point differences
            #         mismatched_remittance_ids.append(result.remittance_id)

            #         mismatch_data = {
            #             "cod_remittance_id": result.remittance_id,
            #             "client_id": result.client_id,
            #             "generated_cod": cod_generated,
            #             "calculated_total_amount": calculated_total,
            #             "difference": cod_generated - calculated_total,
            #             "order_count": result.order_count,
            #             "payout_date": (
            #                 result.payout_date.isoformat()
            #                 if result.payout_date
            #                 else None
            #             ),
            #             "status": result.status,
            #         }
            #         cod_remittance_mismatches.append(mismatch_data)

            # # If there are mismatches, get detailed order information for those remittances only
            # if mismatched_remittance_ids:
            #     orders_for_mismatched_remittances = (
            #         db.query(Order)
            #         .filter(
            #             Order.cod_remittance_cycle_id.in_(mismatched_remittance_ids)
            #         )
            #         .all()
            #     )

            #     # Group orders by remittance ID
            #     orders_by_remittance = {}
            #     for order in orders_for_mismatched_remittances:
            #         remittance_id = order.cod_remittance_cycle_id
            #         if remittance_id not in orders_by_remittance:
            #             orders_by_remittance[remittance_id] = []
            #         orders_by_remittance[remittance_id].append(
            #             {
            #                 "order_id": order.order_id,
            #                 "total_amount": float(order.total_amount),
            #                 "status": order.status,
            #             }
            #         )

            #     # Add order details to mismatch data
            #     for mismatch in cod_remittance_mismatches:
            #         remittance_id = mismatch["cod_remittance_id"]
            #         mismatch["orders"] = orders_by_remittance.get(remittance_id, [])

            # # Sort mismatches by remittance ID for consistent ordering
            # cod_remittance_mismatches.sort(key=lambda x: x["cod_remittance_id"])

            # # Save mismatches to a file for review
            # if cod_remittance_mismatches:
            #     import json
            #     from datetime import datetime

            #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            #     filename = f"cod_remittance_mismatches_{timestamp}.json"
            #     filepath = os.path.join("uploads", filename)

            #     # Ensure uploads directory exists
            #     os.makedirs("uploads", exist_ok=True)

            #     with open(filepath, "w") as f:
            #         json.dump(cod_remittance_mismatches, f, indent=2, default=str)

            #     logger.warning(
            #         f"Found {len(cod_remittance_mismatches)} COD remittance mismatches. Saved to {filepath}"
            #     )

            #     # Also log summary
            #     for mismatch in cod_remittance_mismatches:
            #         logger.warning(
            #             f"COD Remittance ID {mismatch['cod_remittance_id']}: "
            #             f"Generated COD: {mismatch['generated_cod']}, "
            #             f"Calculated Total: {mismatch['calculated_total_amount']}, "
            #             f"Difference: {mismatch['difference']}"
            #         )
            # else:
            #     logger.info(
            #         "All COD remittance entries match their associated orders' total amounts"
            #     )

            # # Return the results for immediate review
            # return GenericResponseModel(
            #     status_code=http.HTTPStatus.OK,
            #     message=f"COD Remittance validation completed. Found {len(cod_remittance_mismatches)} mismatches.",
            #     data={
            #         "total_remittances_checked": len(cod_remittances),
            #         "mismatches_found": len(cod_remittance_mismatches),
            #         "mismatches": (
            #             cod_remittance_mismatches[:10]
            #             if cod_remittance_mismatches
            #             else []
            #         ),  # Show first 10 for immediate review
            #     },
            # )

            # contracts = (
            #     db.query(Company_To_Client_Contract)
            #     .filter(Company_To_Client_Contract.client_id.in_([434]))
            #     .all()
            # )

            # for contract in contracts:
            #     # pull the COD‐rate row (should be exactly one per contract)
            #     cod = (
            #         db.query(Company_To_Client_COD_Rates)
            #         .filter_by(contract_id=contract.id)
            #         .first()
            #     )

            #     # pull all the zone‐rate rows
            #     zone_rows = (
            #         db.query(Company_To_Client_Rates)
            #         .filter_by(contract_id=contract.id)
            #         .all()
            #     )

            #     # initialize all zone columns to None
            #     data = {f"base_rate_zone_{z}": None for z in ["a", "b", "c", "d", "e"]}
            #     data.update(
            #         {
            #             f"additional_rate_zone_{z}": None
            #             for z in ["a", "b", "c", "d", "e"]
            #         }
            #     )
            #     data.update(
            #         {f"rto_base_rate_zone_{z}": None for z in ["a", "b", "c", "d", "e"]}
            #     )
            #     data.update(
            #         {
            #             f"rto_additional_rate_zone_{z}": None
            #             for z in ["a", "b", "c", "d", "e"]
            #         }
            #     )

            #     # pivot the zone rows into the columns
            #     for zr in zone_rows:
            #         z = zr.zone.lower()
            #         data[f"base_rate_zone_{z}"] = zr.base_rate
            #         data[f"additional_rate_zone_{z}"] = zr.additional_rate
            #         data[f"rto_base_rate_zone_{z}"] = zr.rto_base_rate
            #         data[f"rto_additional_rate_zone_{z}"] = zr.rto_additional_rate

            #     # build the new denormalized record
            #     new_rec = New_Company_To_Client_Rate(
            #         uuid=(uuid4()),  # preserve cod_rate.uuid if present
            #         created_at=cod.created_at if cod else datetime.utcnow(),
            #         updated_at=cod.updated_at if cod else datetime.utcnow(),
            #         is_deleted=cod.is_deleted if cod else False,
            #         rate_type=contract.rate_type,  # or another enum/string you use
            #         percentage_rate=cod.percentage_rate if cod else None,
            #         absolute_rate=cod.absolute_rate if cod else None,
            #         isActive=contract.isActive,  # or contract.is_active
            #         company_id=1,
            #         client_id=contract.client_id,
            #         company_contract_id=contract.company_contract_id,
            #         aggregator_courier_id=contract.aggregator_courier_id,
            #         # zone‐based freight/rto rates
            #         **data,
            #     )

            #     db.add(new_rec)

            # db.commit()
            # db.close()
            # print(f"Done: migrated rates for {len(contracts)} contracts.")

            # return GenericResponseModel(
            #     status_code=http.HTTPStatus.OK,
            #     message="Order cloned successfully",
            #     status=True,
            # # )

            # db = get_db_session()

            # from marketplace.easyecom.easyecom_service import EasyEcomService

            # orders = (
            #     db.query(Order)
            #     .filter(
            #         Order.client_id == 310,
            #         # Order.delivered_date > "2025-10-06 00:00:00.000 +0530",
            #         Order.delivered_date > "2025-10-21 00:00:00.000 +0530",
            #         Order.status == "delivered",
            #     )
            #     .all()
            # )

            # print(len(orders), "Total orders")

            # for order in orders:
            #     EasyEcomService.update_order_status_to_easyecom(order)

            # return

            # from shipping_partner.xpressbees.xpressbees import Xpressbees
            # from shipping_partner.delhivery.delhivery import Delhivery
            # from shipping_partner.ats.ats import ATS
            # from shipping_partner.shadowfax.shadowfax import Shadowfax

            # orders = (
            #     db.query(Order)
            #     .filter(
            #         # Order.client_id == 405,
            #         Order.status == "NDR",
            #         Order.aggregator.in_(
            #             ["xpressbees", "delhivery", "amazon", "shadowfax"]
            #         ),
            #     )
            #     .order_by(Order.created_at.desc())
            #     .all()
            # )

            # print("Total orders", len(orders))

            # for order in orders:

            #     if order.aggregator == "xpressbees":
            #         Xpressbees.ndr_action(order, order.awb_number)
            #     elif order.aggregator == "delhivery":
            #         Delhivery.ndr_action(order, order.awb_number)
            #     elif order.aggregator == "ats":
            #         ATS.ndr_action(order, order.awb_number)
            #     elif order.aggregator == "shadowfax":
            #         Shadowfax.ndr_action(order, order.awb_number)

            # print("yayayayayayayayay")

            # return

            # # ADHOC COMPREHENSIVE CLIENT ANALYSIS
            # # Complete analysis directly in clone_order function

            # from datetime import datetime, timedelta
            # import pytz
            # import pandas as pd
            # import os

            # logger.info("Starting adhoc comprehensive client analysis...")

            # ist_timezone = pytz.timezone("Asia/Kolkata")
            # current_time = datetime.now(ist_timezone)

            # # Configuration
            # inactive_days = 15
            # cutoff_date = current_time - timedelta(days=inactive_days)

            # # ========== PHASE 1: ALL CLIENTS ANALYSIS (EXCLUDING NEVER BOOKED) ==========
            # logger.info("Phase 1: Analyzing all clients with shipment history...")

            # # Get ALL clients who have EVER placed orders with AWB (excluding never booked)
            # clients_with_orders = (
            #     db.query(
            #         Client.id,
            #         Client.client_name,
            #         Client.client_code,
            #         Client.is_onboarding_completed,
            #     )
            #     .join(Order, Client.id == Order.client_id)
            #     .filter(
            #         Order.awb_number != None,
            #         Order.awb_number != "",
            #         Order.status != "new",
            #         Order.status != "cancelled",
            #     )
            #     .distinct()
            #     .all()
            # )

            # logger.info(
            #     f"Found {len(clients_with_orders)} clients with shipment history"
            # )

            # # Find recently active clients (with AWB in last 15 days)
            # recent_active_client_ids = set(
            #     db.query(Order.client_id)
            #     .filter(
            #         Order.booking_date >= cutoff_date,
            #         Order.awb_number.isnot(None),
            #         Order.awb_number != "",
            #     )
            #     .distinct()
            #     .all()
            # )
            # recent_active_client_ids = {
            #     client_id[0] for client_id in recent_active_client_ids
            # }

            # logger.info(
            #     f"Found {len(recent_active_client_ids)} recently active clients"
            # )

            # # Analyze each client (both active and inactive)
            # all_clients_analysis = []
            # inactive_analysis = []
            # risk_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
            # monthly_order_data = []  # For monthly order count table

            # for client in clients_with_orders:
            #     # Get client metrics
            #     last_order = (
            #         db.query(Order)
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.awb_number != None,
            #             Order.awb_number != "",
            #             Order.booking_date != None,
            #             Order.status != "new",
            #             Order.status != "cancelled",
            #         )
            #         .order_by(desc(Order.booking_date))
            #         .first()
            #     )

            #     # Calculate days since last order
            #     days_since_last = "No orders found"
            #     last_order_date = None
            #     if last_order and last_order.booking_date:
            #         last_order_date = last_order.booking_date.strftime(
            #             "%Y-%m-%d %H:%M:%S"
            #         )
            #         if last_order.booking_date.tzinfo is None:
            #             last_order_aware = ist_timezone.localize(
            #                 last_order.booking_date
            #             )
            #         else:
            #             last_order_aware = last_order.booking_date
            #         days_since_last = (current_time - last_order_aware).days
            #     else:
            #         # This should not happen since we're only processing clients with orders
            #         logger.warning(
            #             f"Client {client.id} ({client.client_name}) has no last order despite being in clients_with_orders"
            #         )

            #     # Determine if client is currently inactive
            #     is_currently_inactive = client.id not in recent_active_client_ids

            #     # Get order counts and values
            #     total_orders = (
            #         db.query(func.count(Order.id))
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.awb_number.isnot(None),
            #             Order.awb_number != "",
            #         )
            #         .scalar()
            #     ) or 0

            #     total_value = (
            #         db.query(func.coalesce(func.sum(Order.total_amount), 0))
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.awb_number.isnot(None),
            #             Order.awb_number != "",
            #         )
            #         .scalar()
            #     ) or 0

            #     # Get detailed order patterns for different periods
            #     last_15_days = (
            #         db.query(func.count(Order.id))
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.booking_date >= current_time - timedelta(days=15),
            #             Order.awb_number.isnot(None),
            #             Order.awb_number != "",
            #         )
            #         .scalar()
            #     ) or 0

            #     last_30_days = (
            #         db.query(func.count(Order.id))
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.booking_date >= current_time - timedelta(days=30),
            #             Order.awb_number.isnot(None),
            #             Order.awb_number != "",
            #         )
            #         .scalar()
            #     ) or 0

            #     orders_previous_30_days = (
            #         db.query(func.count(Order.id))
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.booking_date >= current_time - timedelta(days=60),
            #             Order.booking_date < current_time - timedelta(days=30),
            #             Order.awb_number.isnot(None),
            #             Order.awb_number != "",
            #         )
            #         .scalar()
            #     ) or 0

            #     orders_60_to_90_days_ago = (
            #         db.query(func.count(Order.id))
            #         .filter(
            #             Order.client_id == client.id,
            #             Order.booking_date >= current_time - timedelta(days=90),
            #             Order.booking_date < current_time - timedelta(days=60),
            #             Order.awb_number.isnot(None),
            #             Order.awb_number != "",
            #         )
            #         .scalar()
            #     ) or 0

            #     # Get monthly order counts for last 6 months
            #     monthly_counts = []
            #     for i in range(6):
            #         # Calculate proper month boundaries
            #         if i == 0:
            #             # Current month
            #             month_start = current_time.replace(
            #                 day=1, hour=0, minute=0, second=0, microsecond=0
            #             )
            #             month_end = current_time
            #         else:
            #             # Previous months
            #             temp_date = current_time.replace(day=1)
            #             for _ in range(i):
            #                 temp_date = (temp_date - timedelta(days=1)).replace(day=1)
            #             month_start = temp_date.replace(
            #                 hour=0, minute=0, second=0, microsecond=0
            #             )
            #             # Calculate end of month
            #             if temp_date.month == 12:
            #                 next_month = temp_date.replace(
            #                     year=temp_date.year + 1, month=1
            #                 )
            #             else:
            #                 next_month = temp_date.replace(month=temp_date.month + 1)
            #             month_end = next_month - timedelta(seconds=1)

            #         monthly_orders = (
            #             db.query(func.count(Order.id))
            #             .filter(
            #                 Order.client_id == client.id,
            #                 Order.booking_date >= month_start,
            #                 Order.booking_date <= month_end,
            #                 Order.awb_number.isnot(None),
            #                 Order.awb_number != "",
            #             )
            #             .scalar()
            #         ) or 0

            #         monthly_counts.append(
            #             {
            #                 "month": month_start.strftime("%Y-%m"),
            #                 "orders": monthly_orders,
            #             }
            #         )

            #     # Calculate average daily orders for different periods
            #     avg_daily_orders_recent_30_days = (
            #         round(last_30_days / 30, 2) if last_30_days > 0 else 0
            #     )
            #     avg_daily_orders_previous_30_days = (
            #         round(orders_previous_30_days / 30, 2)
            #         if orders_previous_30_days > 0
            #         else 0
            #     )
            #     avg_daily_orders_60_to_90_days = (
            #         round(orders_60_to_90_days_ago / 30, 2)
            #         if orders_60_to_90_days_ago > 0
            #         else 0
            #     )

            #     # Calculate volume change percentages
            #     recent_vs_previous_30_days_change = None
            #     previous_30_vs_60_to_90_days_change = None
            #     current_vs_previous_month_change = None

            #     if orders_previous_30_days > 0:
            #         recent_vs_previous_30_days_change = round(
            #             (
            #                 (last_30_days - orders_previous_30_days)
            #                 / orders_previous_30_days
            #             )
            #             * 100,
            #             2,
            #         )

            #     if orders_60_to_90_days_ago > 0:
            #         previous_30_vs_60_to_90_days_change = round(
            #             (
            #                 (orders_previous_30_days - orders_60_to_90_days_ago)
            #                 / orders_60_to_90_days_ago
            #             )
            #             * 100,
            #             2,
            #         )

            #     if len(monthly_counts) >= 2 and monthly_counts[1]["orders"] > 0:
            #         current_vs_previous_month_change = round(
            #             (
            #                 (monthly_counts[0]["orders"] - monthly_counts[1]["orders"])
            #                 / monthly_counts[1]["orders"]
            #             )
            #             * 100,
            #             2,
            #         )

            #     # Add to monthly order data for separate sheet with proper month names as keys
            #     monthly_data_entry = {
            #         "client_id": client.id,
            #         "client_name": client.client_name,
            #         "total_orders": total_orders,
            #         "avg_monthly_orders": (
            #             round(
            #                 sum([m["orders"] for m in monthly_counts])
            #                 / len(monthly_counts),
            #                 2,
            #             )
            #             if monthly_counts
            #             else 0
            #         ),
            #     }

            #     # Add each month's data with the actual month name as key
            #     for i, month_data in enumerate(monthly_counts):
            #         month_name = month_data["month"]
            #         if i == 0:
            #             monthly_data_entry[f"{month_name}_Current"] = month_data[
            #                 "orders"
            #             ]
            #         else:
            #             monthly_data_entry[month_name] = month_data["orders"]

            #     monthly_order_data.append(monthly_data_entry)

            #     # Risk assessment (enhanced for volume drops)
            #     risk_level = "LOW"
            #     volume_drop_risk = False

            #     # Check for significant volume drops
            #     if (
            #         recent_vs_previous_30_days_change
            #         and recent_vs_previous_30_days_change < -50
            #         and last_30_days < orders_previous_30_days
            #         and orders_previous_30_days >= 30
            #     ):
            #         volume_drop_risk = True
            #     elif (
            #         current_vs_previous_month_change
            #         and current_vs_previous_month_change < -60
            #         and monthly_counts[1]["orders"] >= 50
            #     ):
            #         volume_drop_risk = True

            #     # Enhanced risk assessment
            #     if (
            #         (total_value >= 50000 and total_orders >= 50)
            #         or total_value >= 100000
            #         or volume_drop_risk
            #     ):
            #         risk_level = "HIGH"
            #     elif (
            #         (total_value >= 10000 and total_orders >= 20)
            #         or (
            #             isinstance(days_since_last, int)
            #             and days_since_last <= 45
            #             and total_orders >= 10
            #         )
            #         or (
            #             recent_vs_previous_30_days_change
            #             and recent_vs_previous_30_days_change < -30
            #         )
            #     ):
            #         risk_level = "MEDIUM"

            #     # Only count risk for inactive clients
            #     if is_currently_inactive:
            #         risk_counts[risk_level] += 1

            #     client_data = {
            #         "client_id": client.id,
            #         "client_name": client.client_name,
            #         "client_code": client.client_code,
            #         "last_order_date": last_order_date,
            #         "days_since_last_order": days_since_last,
            #         "is_currently_inactive": is_currently_inactive,
            #         "total_orders": total_orders,
            #         "total_order_value": float(total_value),
            #         "orders_last_15_days": last_15_days,
            #         "orders_last_30_days": last_30_days,
            #         "orders_previous_30_days": orders_previous_30_days,
            #         "orders_60_to_90_days_ago": orders_60_to_90_days_ago,
            #         "avg_daily_orders_recent_30_days": avg_daily_orders_recent_30_days,
            #         "avg_daily_orders_previous_30_days": avg_daily_orders_previous_30_days,
            #         "avg_daily_orders_60_to_90_days": avg_daily_orders_60_to_90_days,
            #         "recent_vs_previous_30_days_change_percent": recent_vs_previous_30_days_change,
            #         "previous_30_vs_60_to_90_days_change_percent": previous_30_vs_60_to_90_days_change,
            #         "current_vs_previous_month_change_percent": current_vs_previous_month_change,
            #         "volume_drop_risk": volume_drop_risk,
            #         "risk_level": risk_level,
            #         "is_onboarding_completed": client.is_onboarding_completed,
            #     }

            #     all_clients_analysis.append(client_data)

            #     # Add to inactive analysis if currently inactive
            #     if is_currently_inactive:
            #         inactive_analysis.append(client_data)

            # # Sort all clients analysis by volume drop risk and total value
            # all_clients_analysis.sort(
            #     key=lambda x: (
            #         not x["volume_drop_risk"],  # Volume drop risk first
            #         {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[x["risk_level"]],
            #         -x["total_order_value"],
            #     )
            # )

            # # Sort inactive analysis by risk level and value
            # inactive_analysis.sort(
            #     key=lambda x: (
            #         {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[x["risk_level"]],
            #         -x["total_order_value"],
            #     )
            # )

            # # Sort monthly order data by latest month orders (descending)
            # # Find the current month key (the one ending with '_Current')
            # def get_current_month_orders(entry):
            #     for key, value in entry.items():
            #         if key.endswith("_Current") and isinstance(value, int):
            #             return value
            #     return entry.get("total_orders", 0)

            # monthly_order_data.sort(key=lambda x: -get_current_month_orders(x))

            # # ========== PHASE 2: VOLUME DIP ANALYSIS FOR ALL CLIENTS ==========
            # logger.info("Phase 2: Analyzing volume dips for all clients...")

            # # Identify significant volume decreases from all clients
            # volume_decreases = []
            # critical_dips = []

            # for client_data in all_clients_analysis:
            #     significant_dip = False
            #     dip_details = []

            #     # Check recent 30-day vs previous 30-day comparison
            #     if (
            #         client_data.get("recent_vs_previous_30_days_change_percent")
            #         and client_data["recent_vs_previous_30_days_change_percent"] < -30
            #     ):
            #         if (
            #             client_data.get("orders_previous_30_days", 0) >= 30
            #         ):  # Minimum baseline
            #             significant_dip = True
            #             dip_details.append(
            #                 f"Recent vs Previous 30-day drop: {client_data['recent_vs_previous_30_days_change_percent']}% (from {client_data['orders_previous_30_days']} to {client_data['orders_last_30_days']})"
            #             )

            #     # Check monthly comparison
            #     if (
            #         client_data.get("current_vs_previous_month_change_percent")
            #         and client_data["current_vs_previous_month_change_percent"] < -30
            #     ):
            #         # Get previous month orders from client's monthly data
            #         client_monthly = next(
            #             (
            #                 m
            #                 for m in monthly_order_data
            #                 if m["client_id"] == client_data["client_id"]
            #             ),
            #             {},
            #         )
            #         # Find the previous month orders (first non-current month key)
            #         prev_month_orders = 0
            #         current_month_orders = 0
            #         for key, value in client_monthly.items():
            #             if "_Current" in key:
            #                 current_month_orders = value
            #             elif key not in [
            #                 "client_id",
            #                 "client_name",
            #                 "total_orders",
            #                 "avg_monthly_orders",
            #             ] and isinstance(value, int):
            #                 prev_month_orders = value
            #                 break

            #         if prev_month_orders >= 20:  # Minimum baseline
            #             significant_dip = True
            #             dip_details.append(
            #                 f"Monthly drop: {client_data['current_vs_previous_month_change_percent']}% (from {prev_month_orders} to {current_month_orders})"
            #             )

            #     if significant_dip:
            #         severity = "CRITICAL"
            #         recent_vs_previous_change = client_data.get(
            #             "recent_vs_previous_30_days_change_percent"
            #         )
            #         if recent_vs_previous_change and recent_vs_previous_change > -50:
            #             severity = (
            #                 "HIGH" if recent_vs_previous_change < -40 else "MEDIUM"
            #             )

            #         volume_decrease_entry = {
            #             "client_id": client_data["client_id"],
            #             "client_name": client_data["client_name"],
            #             "client_code": client_data.get("client_code", ""),
            #             "is_currently_inactive": client_data.get(
            #                 "is_currently_inactive", False
            #             ),
            #             "orders_last_30_days": client_data.get(
            #                 "orders_last_30_days", 0
            #             ),
            #             "orders_previous_30_days": client_data.get(
            #                 "orders_previous_30_days", 0
            #             ),
            #             "recent_vs_previous_30_days_change_percent": client_data.get(
            #                 "recent_vs_previous_30_days_change_percent"
            #             ),
            #             "current_vs_previous_month_change_percent": client_data.get(
            #                 "current_vs_previous_month_change_percent"
            #             ),
            #             "avg_daily_orders_recent_30_days": client_data.get(
            #                 "avg_daily_orders_recent_30_days", 0
            #             ),
            #             "avg_daily_orders_previous_30_days": client_data.get(
            #                 "avg_daily_orders_previous_30_days", 0
            #             ),
            #             "total_orders": client_data.get("total_orders", 0),
            #             "total_order_value": client_data.get("total_order_value", 0),
            #             "severity": severity,
            #             "dip_details": "; ".join(dip_details),
            #         }

            #         volume_decreases.append(volume_decrease_entry)

            #         # If it's a critical dip (>60% decrease from high volume)
            #         recent_vs_previous_change = client_data.get(
            #             "recent_vs_previous_30_days_change_percent"
            #         )
            #         change_monthly = client_data.get(
            #             "current_vs_previous_month_change_percent"
            #         )
            #         orders_previous_30 = client_data.get("orders_previous_30_days", 0)

            #         if (
            #             recent_vs_previous_change
            #             and recent_vs_previous_change < -60
            #             and orders_previous_30 >= 100
            #         ) or (change_monthly and change_monthly < -60):
            #             critical_dips.append(volume_decrease_entry)

            # # Sort volume decreases by severity and percentage change
            # volume_decreases.sort(
            #     key=lambda x: (
            #         {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}[x["severity"]],
            #         x["recent_vs_previous_30_days_change_percent"] or 0,
            #     )
            # )

            # volume_summary = {
            #     "total_clients_analyzed": len(all_clients_analysis),
            #     "clients_with_volume_decreases": len(volume_decreases),
            #     "critical_volume_dips": len(critical_dips),
            #     "inactive_clients": len(inactive_analysis),
            # }

            # # ========== PHASE 3: GENERATE INSIGHTS AND RECOMMENDATIONS ==========
            # logger.info("Phase 3: Generating insights...")

            # insights = []
            # recommendations = []

            # # High-risk inactive clients
            # high_risk_count = risk_counts["HIGH"]
            # if high_risk_count > 0:
            #     insights.append(
            #         f"{high_risk_count} high-value clients have become inactive - immediate attention required"
            #     )
            #     recommendations.append(
            #         {
            #             "priority": "CRITICAL",
            #             "action": f"Contact {high_risk_count} high-value inactive clients within 24 hours",
            #             "impact": "High revenue recovery potential",
            #         }
            #     )

            # # Volume decreases (now from comprehensive analysis)
            # volume_decrease_count = volume_summary["clients_with_volume_decreases"]
            # critical_dip_count = volume_summary["critical_volume_dips"]

            # if volume_decrease_count > 0:
            #     insights.append(
            #         f"{volume_decrease_count} clients show significant order volume decreases"
            #     )
            #     recommendations.append(
            #         {
            #             "priority": "HIGH",
            #             "action": f"Investigate {volume_decrease_count} clients with volume decreases",
            #             "impact": "Prevent further client churn and revenue loss",
            #         }
            #     )

            # if critical_dip_count > 0:
            #     insights.append(
            #         f"{critical_dip_count} clients have CRITICAL volume dips (>60% decrease from high baseline)"
            #     )
            #     recommendations.append(
            #         {
            #             "priority": "CRITICAL",
            #             "action": f"Emergency investigation for {critical_dip_count} clients with critical volume dips",
            #             "impact": "Immediate revenue recovery - potential high-value client loss",
            #         }
            #     )

            # # Volume drop analysis for active clients
            # active_with_drops = len(
            #     [
            #         c
            #         for c in all_clients_analysis
            #         if not c["is_currently_inactive"] and c["volume_drop_risk"]
            #     ]
            # )
            # if active_with_drops > 0:
            #     insights.append(
            #         f"{active_with_drops} currently ACTIVE clients show concerning volume drops"
            #     )
            #     recommendations.append(
            #         {
            #             "priority": "HIGH",
            #             "action": f"Proactive engagement with {active_with_drops} active clients showing volume drops",
            #             "impact": "Prevent active clients from becoming inactive",
            #         }
            #     )

            # # Incomplete onboarding
            # incomplete_onboarding = len(
            #     [c for c in inactive_analysis if not c["is_onboarding_completed"]]
            # )
            # if incomplete_onboarding > 0:
            #     insights.append(
            #         f"{incomplete_onboarding} inactive clients have incomplete onboarding"
            #     )
            #     recommendations.append(
            #         {
            #             "priority": "MEDIUM",
            #             "action": f"Complete onboarding for {incomplete_onboarding} clients",
            #             "impact": "Improve client activation rate",
            #         }
            #     )

            # # ========== PHASE 4: EXPORT TO EXCEL ==========
            # logger.info("Phase 4: Exporting to Excel...")

            # timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            # filename = f"comprehensive_client_analysis_{timestamp}.xlsx"
            # filepath = os.path.join("uploads", filename)
            # os.makedirs("uploads", exist_ok=True)

            # try:
            #     with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            #         # Executive Summary
            #         summary_data = [
            #             [
            #                 "Analysis Timestamp",
            #                 current_time.strftime("%Y-%m-%d %H:%M:%S"),
            #             ],
            #             [
            #                 "Total Clients Analyzed",
            #                 volume_summary["total_clients_analyzed"],
            #             ],
            #             ["Total Inactive Clients", volume_summary["inactive_clients"]],
            #             ["High Risk Inactive", risk_counts["HIGH"]],
            #             ["Medium Risk Inactive", risk_counts["MEDIUM"]],
            #             ["Low Risk Inactive", risk_counts["LOW"]],
            #             [
            #                 "Clients with Volume Decreases",
            #                 volume_summary["clients_with_volume_decreases"],
            #             ],
            #             [
            #                 "Critical Volume Dips",
            #                 volume_summary["critical_volume_dips"],
            #             ],
            #             ["Analysis Period (Days)", inactive_days],
            #         ]
            #         pd.DataFrame(summary_data, columns=["Metric", "Value"]).to_excel(
            #             writer, sheet_name="Executive_Summary", index=False
            #         )

            #         # High Priority Inactive
            #         high_priority = [
            #             c for c in inactive_analysis if c["risk_level"] == "HIGH"
            #         ]
            #         if high_priority:
            #             pd.DataFrame(high_priority).to_excel(
            #                 writer, sheet_name="High_Priority_Inactive", index=False
            #             )

            #         # All Inactive Clients
            #         if inactive_analysis:
            #             pd.DataFrame(inactive_analysis).to_excel(
            #                 writer, sheet_name="All_Inactive_Clients", index=False
            #             )

            #         # All Clients Analysis (Active + Inactive)
            #         if all_clients_analysis:
            #             pd.DataFrame(all_clients_analysis).to_excel(
            #                 writer, sheet_name="All_Clients_Analysis", index=False
            #             )

            #         # Monthly Order Counts Table
            #         if monthly_order_data:
            #             # Create a formatted monthly table with actual month names as columns
            #             monthly_df = pd.DataFrame(monthly_order_data)

            #             # The DataFrame will now have actual month names as columns (e.g., "2024-10_Current", "2024-09", "2024-08", etc.)
            #             # Simply export it as is - the month names will be the column headers
            #             monthly_df.to_excel(
            #                 writer, sheet_name="Monthly_Order_Counts", index=False
            #             )

            #         # Volume Decreases (Enhanced)
            #         if volume_decreases:
            #             pd.DataFrame(volume_decreases).to_excel(
            #                 writer, sheet_name="Volume_Decreases", index=False
            #             )

            #         # Critical Volume Dips (Separate sheet for urgent attention)
            #         critical_volume_dips = [
            #             v for v in volume_decreases if v["severity"] == "CRITICAL"
            #         ]
            #         if critical_volume_dips:
            #             pd.DataFrame(critical_volume_dips).to_excel(
            #                 writer, sheet_name="CRITICAL_Volume_Dips", index=False
            #             )

            #         # Recommendations
            #         if recommendations:
            #             pd.DataFrame(recommendations).to_excel(
            #                 writer, sheet_name="Recommendations", index=False
            #             )

            #         # Key Insights
            #         if insights:
            #             pd.DataFrame({"Key_Insights": insights}).to_excel(
            #                 writer, sheet_name="Key_Insights", index=False
            #             )

            #     logger.info(f"Analysis exported to: {filepath}")

            # except Exception as e:
            #     logger.error(f"Error exporting to Excel: {str(e)}")
            #     filepath = None

            # # ========== PHASE 5: LOG RESULTS ==========
            # logger.info("=" * 80)
            # logger.info("COMPREHENSIVE CLIENT ANALYSIS RESULTS")
            # logger.info("=" * 80)
            # logger.info(
            #     f"Analysis completed at: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
            # )
            # logger.info(
            #     f"Total clients analyzed: {volume_summary['total_clients_analyzed']} (excluding never-booked)"
            # )
            # logger.info(
            #     f"Currently inactive clients: {volume_summary['inactive_clients']}"
            # )
            # logger.info(f"High risk inactive: {risk_counts['HIGH']}")
            # logger.info(f"Medium risk inactive: {risk_counts['MEDIUM']}")
            # logger.info(f"Low risk inactive: {risk_counts['LOW']}")
            # logger.info(
            #     f"Clients with volume decreases: {volume_summary['clients_with_volume_decreases']}"
            # )
            # logger.info(
            #     f"CRITICAL volume dips: {volume_summary['critical_volume_dips']}"
            # )
            # logger.info(f"Excel report: {filepath}")

            # logger.info("\nKEY INSIGHTS:")
            # for i, insight in enumerate(insights, 1):
            #     logger.info(f"{i}. {insight}")

            # logger.info("\nIMMEDIATE ACTIONS REQUIRED:")
            # for rec in recommendations:
            #     logger.info(f"• {rec['priority']}: {rec['action']}")

            # # Log top volume dips for immediate attention
            # if volume_decreases:
            #     logger.info(f"\nTOP 5 VOLUME DIPS:")
            #     for i, dip in enumerate(volume_decreases[:5], 1):
            #         logger.info(
            #             f"{i}. {dip['client_name']} (ID: {dip['client_id']}) - {dip['severity']}"
            #         )
            #         logger.info(f"   {dip['dip_details']}")

            # logger.info("=" * 80)

            # # Return comprehensive results
            # return GenericResponseModel(
            #     status_code=http.HTTPStatus.OK,
            #     message=f"Comprehensive client analysis completed. Analyzed {volume_summary['total_clients_analyzed']} clients, found {volume_summary['inactive_clients']} inactive clients and {volume_summary['clients_with_volume_decreases']} clients with volume decreases.",
            #     status=True,
            #     data={
            #         "analysis_timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            #         "summary": {
            #             "total_clients_analyzed": volume_summary[
            #                 "total_clients_analyzed"
            #             ],
            #             "inactive_clients": volume_summary["inactive_clients"],
            #             "high_risk_inactive": risk_counts["HIGH"],
            #             "medium_risk_inactive": risk_counts["MEDIUM"],
            #             "low_risk_inactive": risk_counts["LOW"],
            #             "clients_with_volume_decreases": volume_summary[
            #                 "clients_with_volume_decreases"
            #             ],
            #             "critical_volume_dips": volume_summary["critical_volume_dips"],
            #         },
            #         "high_priority_inactive": [
            #             c for c in inactive_analysis if c["risk_level"] == "HIGH"
            #         ][:10],
            #         "critical_volume_dips": [
            #             v for v in volume_decreases if v["severity"] == "CRITICAL"
            #         ][:10],
            #         "top_volume_decreases": volume_decreases[:10],
            #         "clients_with_volume_drops": [
            #             c for c in all_clients_analysis if c["volume_drop_risk"]
            #         ][:10],
            #         "key_insights": insights,
            #         "recommendations": recommendations,
            #         "excel_report_path": filepath,
            #     },
            # )

            # return

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

            # If order not found, throw an error
            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    data={"order_id": order_id},
                    message="Order does not exist",
                )

            # Retrieve and increment the clone_order_count for the order
            clone_order_count = order.clone_order_count + 1
            order.clone_order_count = clone_order_count

            # update the clone order count in the db
            db.add(order)

            # Generate the new order_id
            new_order_id = f"{order_id}_{clone_order_count}"

            # Check if the new order_id already exists (unlikely, but safeguard)
            existing_order = (
                db.query(Order)
                .filter(
                    Order.order_id == new_order_id,
                    Order.company_id == company_id,
                    Order.client_id == client_id,
                )
                .first()
            )
            if existing_order:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    data={"order_id": new_order_id},
                    message="Duplicate order ID generated. Please try again.",
                )

            order_dict = order.__dict__.copy()

            validated_order_data = cloneOrderModel(**order_dict)
            # Remove courier field if it exists to avoid conflicts
            validated_data_dict = validated_order_data.model_dump()
            if "courier" in validated_data_dict:
                del validated_data_dict["courier"]
            cloned_order = Order(**validated_data_dict)

            cloned_order.order_id = new_order_id
            cloned_order.status = "new"
            cloned_order.sub_status = "new"
            cloned_order.tracking_info = []
            cloned_order.action_history = []

            # Add the cloned order to the database
            db.add(cloned_order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Order cloned successfully",
                status=True,
                data={"new_order_id": new_order_id},
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error cloning Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while cloning the Order.",
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
    def get_all_orders(order_filters: Order_filters):
        try:
            # destruct the filters
            page_number = order_filters.page_number
            batch_size = order_filters.batch_size
            order_status = order_filters.order_status
            current_status = order_filters.current_status
            search_term = order_filters.search_term
            start_date = order_filters.start_date
            end_date = order_filters.end_date
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

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # Build base filters that apply to all queries
            base_filters = [
                Order.company_id == company_id,
                Order.client_id == client_id,
                Order.is_deleted == False,
            ]

            # Build common filters that apply to all queries
            common_filters = []

            # Search filter
            if search_term:
                search_terms = [term.strip() for term in search_term.split(",")]
                common_filters.append(
                    or_(
                        *[
                            or_(
                                Order.order_id == term,
                                Order.awb_number == term,
                                Order.consignee_phone == term,
                                Order.consignee_alternate_phone == term,
                                Order.consignee_email == term,
                            )
                            for term in search_terms
                        ]
                    )
                )

            # Date filters
            if date_type == "order date":
                common_filters.extend(
                    [
                        cast(Order.order_date, DateTime) >= start_date,
                        cast(Order.order_date, DateTime) <= end_date,
                    ]
                )
            elif date_type == "booking date":
                common_filters.extend(
                    [
                        cast(Order.booking_date, DateTime) >= start_date,
                        cast(Order.booking_date, DateTime) <= end_date,
                    ]
                )

            # Initialize sku_params for parameter binding
            sku_params = {}

            # Remaining filters (applied to main query and count query)
            remaining_filters = []

            if current_status:
                remaining_filters.append(Order.sub_status == current_status)

            if sku_codes:
                sku_codes = [term.strip() for term in sku_codes.split(",")]
                like_conditions = [
                    text(
                        f"EXISTS (SELECT 1 FROM jsonb_array_elements(products) AS elem WHERE elem->>'sku_code' ILIKE :sku_{i})"
                    )
                    for i, sku in enumerate(sku_codes)
                ]
                sku_filter = or_(*like_conditions)
                remaining_filters.append(sku_filter)

                # Store parameters for later use
                sku_params = {f"sku_{i}": f"%{sku}%" for i, sku in enumerate(sku_codes)}

            if product_name:
                product_names = [term.strip() for term in product_name.split(",")]
                name_filters = [
                    cast(Order.products, String).ilike(f'%"name": "%{name.strip()}%"%')
                    for name in product_names
                ]
                remaining_filters.append(or_(*name_filters))

            if pincode:
                pincodes = [term.strip() for term in pincode.split(",")]
                remaining_filters.append(
                    or_(*[Order.consignee_pincode == p for p in pincodes])
                )

            if pickup_location:
                remaining_filters.append(Order.pickup_location_code == pickup_location)

            if order_id:
                order_ids = [term.strip() for term in order_id.split(",")]
                remaining_filters.append(Order.order_id.in_(order_ids))

            if payment_mode:
                remaining_filters.append(Order.payment_mode == payment_mode)

            if courier_filter:
                remaining_filters.append(Order.courier_partner == courier_filter)

            if product_quantity:
                remaining_filters.append(Order.product_quantity == product_quantity)

            if tags and len(tags) > 0:
                tag_filter = cast(Order.order_tags, String).ilike(f"%{tags.strip()}%")
                remaining_filters.append(tag_filter)

            # Repeat customer filter - if filter is on, only repeat customers, otherwise all
            if repeat_customer is True:
                # Show only repeat customers (customers with more than 1 order)
                repeat_customer_subquery = (
                    db.query(Order.consignee_phone)
                    .filter(
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        Order.consignee_phone.isnot(None),
                        Order.consignee_phone != "",
                    )
                    .group_by(Order.consignee_phone)
                    .having(func.count(Order.id) > 1)
                    .subquery()
                )
                remaining_filters.append(
                    Order.consignee_phone.in_(
                        db.query(repeat_customer_subquery.c.consignee_phone)
                    )
                )

            # Build the base query for status counts (without status filter)
            status_count_query = db.query(Order.status, func.count(Order.id))
            # apply all the filters to the status counts query as well
            status_count_query = status_count_query.filter(*base_filters)
            status_count_query = status_count_query.filter(*common_filters)
            status_count_query = status_count_query.filter(*remaining_filters)

            # Get status counts optimized - count by status without loading data
            status_count_query_final = status_count_query

            if sku_codes:
                status_count_query_final = status_count_query_final.params(**sku_params)

            status_counts_result = status_count_query_final.group_by(Order.status).all()
            status_counts = {status: count for status, count in status_counts_result}

            # Get total count for "all" status
            total_all_count = sum(status_counts.values())
            status_counts["all"] = total_all_count

            # Build main query with status filter
            main_query = db.query(Order)
            main_query = main_query.filter(*base_filters)
            main_query = main_query.filter(*common_filters)
            main_query = main_query.filter(*remaining_filters)

            if sku_codes:
                main_query = main_query.params(**sku_params)

            # Apply status filter to main query only
            if order_status != "all":
                main_query = main_query.filter(Order.status == order_status)

            # Get distinct courier partners efficiently (include status filter)
            courier_query = db.query(Order.courier_partner).filter(*base_filters)
            courier_query = courier_query.filter(*common_filters)
            courier_query = courier_query.filter(*remaining_filters)

            # Apply status filter to courier query
            if order_status != "all":
                courier_query = courier_query.filter(Order.status == order_status)

            courier_query = courier_query.filter(
                Order.courier_partner.isnot(None), Order.courier_partner != ""
            ).distinct()

            if sku_codes:
                courier_query = courier_query.params(**sku_params)

            distinct_courier_partners = [partner[0] for partner in courier_query.all()]

            # Get total count for pagination
            total_count = main_query.count()

            # Apply pagination and sorting
            main_query = main_query.order_by(
                desc(Order.order_date), desc(Order.created_at), desc(Order.id)
            )
            offset_value = (page_number - 1) * batch_size
            main_query = main_query.offset(offset_value).limit(batch_size)

            # Execute main query with joinedload for pickup_location
            fetched_orders = main_query.options(joinedload(Order.pickup_location)).all()

            ####### REPEAT CUSTOMER LOGIC #######

            # Get previous order counts for all phone numbers in a single query
            phone_numbers = [
                order.consignee_phone
                for order in fetched_orders
                if order.consignee_phone
            ]
            previous_order_counts = {}

            if phone_numbers:
                # Query to count orders per phone number (excluding current orders)
                phone_count_query = (
                    db.query(Order.consignee_phone, func.count(Order.id))
                    .filter(
                        Order.consignee_phone.in_(phone_numbers),
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                    )
                    .group_by(Order.consignee_phone)
                    .all()
                )

                previous_order_counts = {
                    phone: count for phone, count in phone_count_query
                }

            # Convert orders to response format
            fetched_orders_response = []
            for order in fetched_orders:
                order_dict = order.to_model().model_dump()

                # Get actual previous order count for this phone number
                phone = order.consignee_phone
                total_orders_for_phone = previous_order_counts.get(phone, 1)

                # Previous orders = total orders - 1 (current order)
                order_dict["previous_order_count"] = max(0, total_orders_for_phone - 1)

                fetched_orders_response.append(Order_Response_Model(**order_dict))

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                data={
                    "orders": fetched_orders_response,
                    "total_count": total_count,
                    "courier_filter": distinct_courier_partners,
                    "status_counts": status_counts,
                },
                status=True,
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
    def get_previous_orders(order_id: str, page_number: int = 1, batch_size: int = 10):
        """
        Get previous orders for the same phone number as the given order ID with pagination
        """
        try:
            with get_db_session() as db:
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # First, get the current order to extract the phone number
                current_order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_id,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                    )
                    .first()
                )

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

                # Build base query for previous orders
                base_query = db.query(Order).filter(
                    Order.consignee_phone == current_order.consignee_phone,
                    Order.company_id == company_id,
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                    Order.order_id != order_id,  # Exclude current order
                )

                # Get status counts for previous orders
                status_count_query = (
                    db.query(Order.status, func.count(Order.id))
                    .filter(
                        Order.consignee_phone == current_order.consignee_phone,
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                        Order.order_id != order_id,  # Exclude current order
                    )
                    .group_by(Order.status)
                    .all()
                )

                # Initialize status counts
                status_counts = {status: count for status, count in status_count_query}

                # Group statuses into 3 main categories
                rto_count = status_counts.get("rto_delivered", 0) + status_counts.get(
                    "rto", 0
                )
                delivered_count = status_counts.get("delivered", 0)
                other_count = sum(
                    count
                    for status, count in status_counts.items()
                    if status not in ["rto_delivered", "rto", "delivered"]
                )

                # Create categorized status counts
                categorized_status_counts = {
                    "rto": rto_count,
                    "delivered": delivered_count,
                    "others": other_count,
                    "total": sum(status_counts.values()),
                }

                # Get total count for pagination
                total_count = base_query.count()

                # Apply pagination and sorting
                offset_value = (page_number - 1) * batch_size
                previous_orders = (
                    base_query.options(joinedload(Order.pickup_location))
                    .order_by(
                        desc(Order.order_date), desc(Order.created_at), desc(Order.id)
                    )
                    .offset(offset_value)
                    .limit(batch_size)
                    .all()
                )

                # Convert to response format
                previous_orders_response = []
                for order in previous_orders:
                    order_dict = order.to_model().model_dump()
                    previous_orders_response.append(Order_Response_Model(**order_dict))

                # Calculate pagination info
                total_pages = (total_count + batch_size - 1) // batch_size
                has_next = page_number < total_pages
                has_prev = page_number > 1

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Previous orders fetched successfully",
                    data={
                        "current_order_id": order_id,
                        "phone_number": current_order.consignee_phone,
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
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fetching previous orders: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching previous orders.",
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
    def export_orders(order_filters: Order_filters):

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

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # query the db for fetching the orders

            query = db.query(Order)

            if client_id == 85:

                query = query.filter(
                    Order.is_deleted == False,
                )

            else:
                query = query.filter(
                    Order.company_id == company_id,
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                )

            if search_term:
                search_terms = [
                    term.strip() for term in search_term.split(",")
                ]  # Split and remove whitespace
                query = query.filter(
                    or_(
                        *[
                            or_(
                                Order.order_id == term,
                                Order.awb_number == term,
                                Order.consignee_phone == term,
                                Order.consignee_alternate_phone == term,
                                Order.consignee_email == term,
                            )
                            for term in search_terms
                        ]
                    )
                )
            # date range filter

            query = query.filter(
                cast(Order.order_date, DateTime) >= start_date,
                cast(Order.order_date, DateTime) <= end_date,
            )
            print(query.count())

            if sku_codes:

                sku_codes = [term.strip() for term in sku_codes.split(",")]

                like_conditions = [
                    cast(Order.products, String).like(f'%"sku_code": "{sku}"%')
                    for sku in sku_codes
                ]

                # Filter orders based on the conditions
                query = query.filter(
                    or_(*like_conditions)  # Use 'or_' to combine the LIKE conditions
                )

            if payment_mode:
                query = query.filter(Order.payment_mode == payment_mode)

            if courier_filter:
                query = query.filter(Order.courier_partner == courier_filter)

            # status filter
            if order_status != "all":
                query = query.filter(Order.status == order_status)

            if order_id:
                order_ids = [
                    term.strip() for term in order_id.split(",")
                ]  # Split and trim spaces
                query = query.filter(Order.order_id.in_(order_ids))

            # fetch the orders in descending order of order date
            query = query.order_by(desc(Order.order_date), desc(Order.created_at))

            fetched_orders = query.options(joinedload(Order.pickup_location)).all()

            orders_data = []
            client_name_dict = {}

            if client_id == 85:
                client_ids = [order.client_id for order in fetched_orders]
                clients = (
                    db.query(Client.id, Client.client_name)
                    .filter(Client.id.in_(client_ids))
                    .all()
                )
                client_name_dict = {client.id: client.client_name for client in clients}

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
                            if order.pickup_location.location_type != ""
                            else ""
                        ),
                        "alternate_phone": (
                            order.pickup_location.alternate_phone
                            if order.pickup_location.alternate_phone != ""
                            else ""
                        ),
                        "address": (
                            order.pickup_location.address
                            if order.pickup_location.address != ""
                            else ""
                        ),
                        "location_code": (
                            order.pickup_location.location_code
                            if order.pickup_location.location_code != ""
                            else ""
                        ),
                        "contact_person_name": (
                            order.pickup_location.contact_person_name
                            if order.pickup_location.contact_person_name != ""
                            else ""
                        ),
                        "pincode": (
                            order.pickup_location.pincode
                            if order.pickup_location.pincode != ""
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

                if client_id == 85:

                    client_name = client_name_dict.get(order.client_id, "")

                    body = {"client_name": client_name, **body}

                    body["aggregator"] = order.aggregator if order.aggregator else ""

                    body["Courier Partner"] = (
                        order.courier_partner if order.courier_partner else ""
                    )

                for index, product in enumerate(order.products, start=1):
                    body[f"Product {index} Name"] = product.get("name", "")
                    body[f"Product {index} Quantity"] = product.get("quantity", "")
                    body[f"Product {index} SKU Code"] = product.get("sku_code", "")
                    body[f"Product {index} Unit Price"] = product.get("unit_price", "")

                orders_data.append(body)

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

    # @staticmethod
    # def get_order_status_counts(order_status_filter):

    #     try:
    #         search_term = order_status_filter.search_term
    #         start_date = order_status_filter.start_date
    #         end_date = order_status_filter.end_date
    #         date_type = order_status_filter.date_type

    #         with get_db_session() as db:

    #             company_id = context_user_data.get().company_id
    #             client_id = context_user_data.get().client_id

    #             # query the db for fetching the orders

    #             query = db.query(Order)

    #             if client_id != 85:

    #                 # applying company and client filter
    #                 query = query.filter(
    #                     Order.company_id == company_id,
    #                     Order.client_id == client_id,
    #                     Order.is_deleted == False,
    #                 )

    #             else:
    #                 # applying company and client filter
    #                 query = query.filter(Order.is_deleted == False)

    #             # if search term is present, give it the highest priority and no other filter will be applied

    #             if search_term:
    #                 search_terms = [
    #                     term.strip() for term in search_term.split(",")
    #                 ]  # Split and remove whitespace
    #                 query = query.filter(
    #                     or_(
    #                         *[
    #                             or_(
    #                                 Order.order_id == term,
    #                                 Order.awb_number == term,
    #                                 Order.consignee_phone == term,
    #                                 Order.consignee_alternate_phone == term,
    #                                 Order.consignee_email == term,
    #                             )
    #                             for term in search_terms
    #                         ]
    #                     )
    #                 )

    #             # date range filter

    #             if date_type == "order date":
    #                 query = query.filter(
    #                     cast(Order.order_date, DateTime) >= start_date,
    #                     cast(Order.order_date, DateTime) <= end_date,
    #                 )

    #             elif date_type == "booking date":
    #                 query = query.filter(
    #                     cast(Order.booking_date, DateTime) >= start_date,
    #                     cast(Order.booking_date, DateTime) <= end_date,
    #                 )

    #             # Get counts for each status before applying the status filter
    #             status_counts = (
    #                 query.with_entities(Order.status, func.count(Order.id))
    #                 .group_by(Order.status)  # Group by status
    #                 .all()
    #             )

    #             # create a dictionary for the status counts
    #             status_counts = {status: count for status, count in status_counts}
    #             status_counts["all"] = query.count()

    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 message="Orders fetched Successfully",
    #                 data={
    #                     "status_counts": status_counts,
    #                 },
    #                 status=True,
    #             )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Error fecthing Order Statuses: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An error occurred while fetchin the Order Statuses.",
    #         )

    #     except Exception as e:
    #         # Log other unhandled exceptions
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Unhandled error: {}".format(str(e)),
    #         )
    #         # Return a general internal server error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An internal server error occurred. Please try again later.",
    #         )

    @staticmethod
    def get_order_by_Id(order_id):

        try:

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            with get_db_session() as db:

                order_data = (
                    db.query(Order)
                    .filter(
                        Order.company_id == company_id,
                        Order.client_id == client_id,
                        Order.order_id == order_id,
                        Order.is_deleted == False,
                    )
                    .options(joinedload(Order.pickup_location))
                    .first()
                )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Orders fetched Successfully",
                    data=Single_Order_Response_Model(
                        **order_data.to_model().model_dump()
                    ),
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
