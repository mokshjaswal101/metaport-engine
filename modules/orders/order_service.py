import http
from uuid import uuid4

from sqlalchemy import (
    or_,
    desc,
    asc,
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
from sqlalchemy.orm import joinedload, selectinload
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
from utils.datetime import convert_to_utc
from utils.string import round_to_2_decimal_place
from modules.pickup_location.pickup_location_service import serialize_pickup_location

from logger import logger

# schema
from schema.base import GenericResponseModel
from .order_schema import (
    Order_create_request_model,
    Order_Response_Model,
    Single_Order_Response_Model,
    OrderItemResponse,
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
    OrderItem,
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


class OrderService:

    @staticmethod
    def _parse_validation_error(error_message: str) -> str:

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
        """
        Create a new order with products stored in order_item table
        and audit logging in order_audit_log table.
        """
        from models.order_item import OrderItem
        from models.order_audit_log import OrderAuditLog

        try:
            db = get_db_session()

            client_id = context_user_data.get().client_id
            user_id = context_user_data.get().id
            user_name = context_user_data.get().first_name

            # Check if order_id already exists for this client
            existing_order = (
                db.query(Order)
                .filter(
                    Order.order_id == order_data.order_id,
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                )
                .first()
            )

            if existing_order:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    data={"order_id": order_data.order_id},
                    message="Order ID already exists",
                )

            # Extract products before converting to dict
            products = order_data.products or []
            order_dict = order_data.model_dump(exclude={"products"})

            # Add client id
            order_dict["client_id"] = client_id

            # Calculate order_value from products
            order_value = (
                sum(
                    float(
                        p.unit_price
                        if hasattr(p, "unit_price")
                        else p.get("unit_price", 0)
                    )
                    * int(
                        p.quantity if hasattr(p, "quantity") else p.get("quantity", 1)
                    )
                    for p in products
                )
                if products
                else 0
            )

            # Calculate total_amount
            total_amount = (
                float(order_dict.get("shipping_charges") or 0)
                + float(order_dict.get("cod_charges") or 0)
                + float(order_dict.get("gift_wrap_charges") or 0)
                + float(order_dict.get("other_charges") or 0)
                + float(order_dict.get("tax_amount") or 0)
                + order_value
                - float(order_dict.get("discount") or 0)
            )

            order_dict["order_value"] = round(order_value, 2)
            order_dict["total_amount"] = round(total_amount, 2)

            # Set COD to collect for COD orders
            if order_dict.get("payment_mode", "").upper() == "COD":
                provided_cod_to_collect = order_dict.get("cod_to_collect")
                if (
                    provided_cod_to_collect is not None
                    and float(provided_cod_to_collect) > 0
                ):
                    # Validate cod_to_collect doesn't exceed total_amount
                    if float(provided_cod_to_collect) > order_dict["total_amount"]:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message=f"COD to collect (₹{provided_cod_to_collect}) cannot exceed total amount (₹{order_dict['total_amount']})",
                            status=False,
                        )
                    order_dict["cod_to_collect"] = round(
                        float(provided_cod_to_collect), 2
                    )
                else:
                    order_dict["cod_to_collect"] = order_dict["total_amount"]
            else:
                order_dict["cod_to_collect"] = 0

            # Calculate volumetric and applicable weight
            volumetric_weight = round(
                (order_dict["length"] * order_dict["breadth"] * order_dict["height"])
                / 5000,
                3,
            )
            applicable_weight = round(max(order_dict["weight"], volumetric_weight), 3)

            order_dict["applicable_weight"] = applicable_weight
            order_dict["volumetric_weight"] = volumetric_weight

            # Set initial status
            order_dict["status"] = "new"
            order_dict["sub_status"] = "new"

            # Convert order_date to UTC
            order_dict["order_date"] = convert_to_utc(
                order_date=order_dict["order_date"]
            )

            # Fetch the pickup location pincode
            pickup_result = (
                db.query(Pickup_Location.pincode)
                .filter(
                    Pickup_Location.location_code == order_dict["pickup_location_code"],
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_deleted == False,
                )
                .first()
            )

            if pickup_result is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pickup Location",
                )

            pickup_pincode = pickup_result[0]

            # Calculate shipping zone
            zone_data = ShipmentService.calculate_shipping_zone(
                pickup_pincode, order_dict["consignee_pincode"]
            )

            if not zone_data.status:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pincodes",
                    status=False,
                )

            order_dict["zone"] = zone_data.data["zone"]

            # Create order entity
            order_model_instance = Order.create_db_entity(order_dict)
            db.add(order_model_instance)
            db.flush()  # Get the order ID without committing

            # Create order items from products
            if products:
                order_items = OrderItem.bulk_create_from_products(
                    order_id=order_model_instance.id,
                    products=[
                        p.model_dump() if hasattr(p, "model_dump") else p
                        for p in products
                    ],
                )
                db.add_all(order_items)

            # Create audit log entry
            audit_log = OrderAuditLog.log_order_created(
                order_id=order_model_instance.id,
                user_id=user_id,
                user_name=user_name,
                source="platform",
            )
            db.add(audit_log)

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Order created Successfully",
                data={"order_id": order_model_instance.order_id},
                status=True,
            )

        except DatabaseError as e:
            db.rollback()
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
            )

        except Exception as e:
            db.rollback()
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def update_order(order_id: str, order_data: Order_create_request_model):
        """
        Update an existing order. Only orders with status 'new' can be updated.
        Products are updated in order_item table, audit log in order_audit_log table.
        """
        from models.order_item import OrderItem
        from models.order_audit_log import OrderAuditLog

        try:
            db = get_db_session()

            client_id = context_user_data.get().client_id
            user_id = context_user_data.get().id
            user_name = context_user_data.get().name

            # Find the existing order
            order = (
                db.query(Order)
                .filter(
                    Order.order_id == order_id,
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                )
                .first()
            )

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    data={"order_id": order_data.order_id},
                    message="Order does not exist",
                )

            if order.status != "new":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Order already processed, cannot be updated",
                )

            # Check if order_id is being changed and new one already exists
            if order_id != order_data.order_id:
                check_existing_order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_data.order_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
                    )
                    .first()
                )

                if check_existing_order:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        data={"order_id": order_data.order_id},
                        message="This order id already exists for another order",
                    )

            # Extract products before converting to dict
            products = order_data.products or []
            order_dict = order_data.model_dump(exclude={"products"})

            # Calculate order_value from products
            order_value = (
                sum(
                    float(
                        p.unit_price
                        if hasattr(p, "unit_price")
                        else p.get("unit_price", 0)
                    )
                    * int(
                        p.quantity if hasattr(p, "quantity") else p.get("quantity", 1)
                    )
                    for p in products
                )
                if products
                else 0
            )

            # Calculate total_amount
            total_amount = (
                float(order_dict.get("shipping_charges") or 0)
                + float(order_dict.get("cod_charges") or 0)
                + float(order_dict.get("gift_wrap_charges") or 0)
                + float(order_dict.get("other_charges") or 0)
                + float(order_dict.get("tax_amount") or 0)
                + order_value
                - float(order_dict.get("discount") or 0)
            )

            order_dict["order_value"] = round(order_value, 2)
            order_dict["total_amount"] = round(total_amount, 2)

            # Set COD to collect for COD orders
            if order_dict.get("payment_mode", "").upper() == "COD":
                provided_cod_to_collect = order_dict.get("cod_to_collect")
                if (
                    provided_cod_to_collect is not None
                    and float(provided_cod_to_collect) > 0
                ):
                    # Validate cod_to_collect doesn't exceed total_amount
                    if float(provided_cod_to_collect) > order_dict["total_amount"]:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message=f"COD to collect (₹{provided_cod_to_collect}) cannot exceed total amount (₹{order_dict['total_amount']})",
                            status=False,
                        )
                    order_dict["cod_to_collect"] = round(
                        float(provided_cod_to_collect), 2
                    )
                else:
                    order_dict["cod_to_collect"] = order_dict["total_amount"]
            else:
                order_dict["cod_to_collect"] = 0

            # Fields that should not be updated directly
            exclude_fields = {
                "client_id",
                "status",
                "sub_status",
                "zone",
                "applicable_weight",
                "volumetric_weight",
            }

            # Update order fields
            for key, value in order_dict.items():
                if key not in exclude_fields and hasattr(order, key):
                    setattr(order, key, value)

            # Calculate volumetric and applicable weight
            volumetric_weight = round(
                (order_dict["length"] * order_dict["breadth"] * order_dict["height"])
                / 5000,
                3,
            )
            applicable_weight = round(max(order_dict["weight"], volumetric_weight), 3)

            order.applicable_weight = applicable_weight
            order.volumetric_weight = volumetric_weight

            # Convert order_date to UTC
            order.order_date = convert_to_utc(order_date=order.order_date)

            # Fetch the pickup location pincode
            pickup_result = (
                db.query(Pickup_Location.pincode)
                .filter(
                    Pickup_Location.location_code == order_dict["pickup_location_code"],
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_deleted == False,
                )
                .first()
            )

            if pickup_result is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pickup Location",
                )

            pickup_pincode = pickup_result[0]

            # Calculate shipping zone
            zone_data = ShipmentService.calculate_shipping_zone(
                pickup_pincode, order_dict["consignee_pincode"]
            )

            if not zone_data.status:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pincodes",
                    status=False,
                )

            order.zone = zone_data.data.get("zone", "E")
            order.sub_status = "new"

            # Replace order items: delete existing and create new ones
            if products:
                db.query(OrderItem).filter(OrderItem.order_id == order.id).delete(
                    synchronize_session=False
                )
                db.add_all(
                    OrderItem.bulk_create_from_products(
                        order_id=order.id,
                        products=[
                            p.model_dump() if hasattr(p, "model_dump") else p
                            for p in products
                        ],
                    )
                )

            # Create audit log entry
            audit_log = OrderAuditLog.log_order_updated(
                order_id=order.id,
                user_id=user_id,
                user_name=user_name,
            )
            db.add(audit_log)

            # Commit all changes
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Order updated Successfully",
                status=True,
            )

        except DatabaseError as e:
            db.rollback()
            logger.error(
                extra=context_user_data.get(),
                msg="Error updating Order: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while updating the Order.",
            )

        except Exception as e:
            db.rollback()
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
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

            client_id = context_user_data.get().client_id

            # Find the existing order from the db

            order = (
                db.query(Order)
                .filter(
                    Order.order_id == order_id,
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

            from models.order_item import OrderItem
            from models.order_audit_log import OrderAuditLog

            user_id = context_user_data.get().id
            user_name = context_user_data.get().name

            order_dict = order.__dict__.copy()

            # Remove SQLAlchemy internal state and fields that shouldn't be copied
            order_dict.pop("_sa_instance_state", None)
            order_dict.pop("id", None)
            order_dict.pop("uuid", None)
            order_dict.pop("created_at", None)
            order_dict.pop("updated_at", None)

            validated_order_data = cloneOrderModel(**order_dict)
            # Remove fields that don't exist on Order model
            validated_data_dict = validated_order_data.model_dump()
            validated_data_dict.pop("products", None)
            validated_data_dict.pop("courier", None)

            cloned_order = Order(**validated_data_dict)

            cloned_order.order_id = new_order_id
            cloned_order.status = "new"
            cloned_order.sub_status = "new"

            # Clear shipment-related fields
            cloned_order.awb_number = None
            cloned_order.courier_partner = None
            cloned_order.aggregator = None
            cloned_order.shipping_partner_order_id = None
            cloned_order.shipping_partner_shipping_id = None
            cloned_order.label_url = None
            cloned_order.manifest_url = None
            cloned_order.invoice_url = None
            cloned_order.is_label_generated = False

            # Add the cloned order to the database
            db.add(cloned_order)
            db.flush()  # Get the ID

            # Clone order items from original order
            original_items = (
                db.query(OrderItem)
                .filter(
                    OrderItem.order_id == order.id,
                    OrderItem.is_deleted == False,
                )
                .all()
            )

            if original_items:
                cloned_items = []
                for item in original_items:
                    cloned_item = OrderItem(
                        order_id=cloned_order.id,
                        name=item.name,
                        sku_code=item.sku_code,
                        quantity=item.quantity,
                        unit_price=item.unit_price,
                        product_id=item.product_id,
                    )
                    cloned_items.append(cloned_item)
                db.add_all(cloned_items)

            # Create audit log entry
            audit_log = OrderAuditLog.log_order_cloned(
                order_id=cloned_order.id,
                source_order_id=order_id,
                user_id=user_id,
                user_name=user_name,
            )
            db.add(audit_log)

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
        """Cancel an order. Only orders with status 'new' can be cancelled."""
        from models.order_audit_log import OrderAuditLog

        try:
            with get_db_session() as db:

                client_id = context_user_data.get().client_id
                user_id = context_user_data.get().id
                user_name = context_user_data.get().name

                # Find the existing order from the db
                order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_id,
                        Order.client_id == client_id,
                        Order.is_deleted == False,
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
                order.cancel_count = (order.cancel_count or 0) + 1

                # Create audit log entry
                audit_log = OrderAuditLog.log_order_cancelled(
                    order_id=order.id,
                    user_id=user_id,
                    user_name=user_name,
                )
                db.add(audit_log)

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
                "status": "new",
                "sub_status": "new",
                "current_timestamp": current_timestamp,
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

                    # OPTIMIZATION 8: Bulk update with pre-computed constants
                    order_dict.update(VALIDATION_CONSTANTS)
                    order_dict.update(
                        {
                            "zone": zone,
                            "applicable_weight": applicable_weight,
                            "volumetric_weight": volumetric_weight,
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

                                # Prepare the insert tuple with all required fields
                                insert_values.append(
                                    (
                                        # Database required fields
                                        uuid.uuid4(),  # uuid
                                        # Basic order info
                                        str(order_dict.get("order_id", "")),
                                        order_date_str,
                                        order_dict.get("channel"),
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
                                                "is_billing_same_as_consignee", True
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
                                        # Package dimensions
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
                                    uuid, order_id, order_date, channel, client_id,
                                    consignee_full_name, consignee_phone, consignee_alternate_phone, consignee_email,
                                    consignee_company, consignee_gstin, consignee_address, consignee_landmark,
                                    consignee_pincode, consignee_city, consignee_state, consignee_country,
                                    is_billing_same_as_consignee, billing_full_name, billing_phone, billing_email,
                                    billing_address, billing_landmark, billing_pincode, billing_city, billing_state, billing_country,
                                    pickup_location_code, payment_mode, total_amount, order_value,
                                    shipping_charges, cod_charges, discount, gift_wrap_charges, other_charges, tax_amount,
                                    invoice_number, invoice_date, invoice_amount, eway_bill_number,
                                    length, breadth, height, weight, applicable_weight, volumetric_weight,
                                    zone, status, sub_status,
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

            client_id = context_user_data.get().client_id

            # Build base filters that apply to all queries
            # Note: company_id removed from Order model - using client_id only
            base_filters = [
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

            # Remaining filters (applied to main query and count query)
            remaining_filters = []

            if current_status:
                remaining_filters.append(Order.sub_status == current_status)

            if sku_codes:
                sku_codes_list = [term.strip() for term in sku_codes.split(",")]
                # Use EXISTS subquery to filter orders by SKU code in order_item table
                sku_conditions = [
                    OrderItem.sku_code.ilike(f"%{sku}%") for sku in sku_codes_list
                ]
                sku_exists = (
                    db.query(OrderItem.id)
                    .filter(
                        OrderItem.order_id == Order.id,
                        or_(*sku_conditions),
                        OrderItem.is_deleted == False,
                    )
                    .exists()
                )
                remaining_filters.append(sku_exists)

            if product_name:
                product_names = [term.strip() for term in product_name.split(",")]
                # Use EXISTS subquery to filter orders by product name in order_item table
                name_conditions = [
                    OrderItem.name.ilike(f"%{name}%") for name in product_names
                ]
                name_exists = (
                    db.query(OrderItem.id)
                    .filter(
                        OrderItem.order_id == Order.id,
                        or_(*name_conditions),
                        OrderItem.is_deleted == False,
                    )
                    .exists()
                )
                remaining_filters.append(name_exists)

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
                # Use scalar subquery to filter orders by total product quantity from order_item table
                qty_scalar = (
                    db.query(func.coalesce(func.sum(OrderItem.quantity), 0))
                    .filter(
                        OrderItem.order_id == Order.id,
                        OrderItem.is_deleted == False,
                    )
                    .correlate(Order)
                    .scalar_subquery()
                )
                remaining_filters.append(qty_scalar == product_quantity)

            # Note: order_tags filter temporarily disabled - tags moved to separate table
            # if tags and len(tags) > 0:
            #     tag_filter = cast(Order.order_tags, String).ilike(f"%{tags.strip()}%")
            #     remaining_filters.append(tag_filter)

            # Repeat customer filter - if filter is on, only repeat customers, otherwise all
            if repeat_customer is True:
                # Show only repeat customers (customers with more than 1 order)
                repeat_customer_subquery = (
                    db.query(Order.consignee_phone)
                    .filter(
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
            status_counts_result = status_count_query.group_by(Order.status).all()
            status_counts = {status: count for status, count in status_counts_result}

            # Get total count for "all" status
            total_all_count = sum(status_counts.values())
            status_counts["all"] = total_all_count

            # Build main query with status filter
            main_query = db.query(Order)
            main_query = main_query.filter(*base_filters)
            main_query = main_query.filter(*common_filters)
            main_query = main_query.filter(*remaining_filters)

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

            distinct_courier_partners = [partner[0] for partner in courier_query.all()]

            # Get total count for pagination
            total_count = main_query.count()

            # Apply pagination and sorting
            main_query = main_query.order_by(
                desc(Order.order_date), desc(Order.created_at), desc(Order.id)
            )
            offset_value = (page_number - 1) * batch_size
            main_query = main_query.offset(offset_value).limit(batch_size)

            # Execute main query with joinedload for pickup_location and selectinload for items
            fetched_orders = main_query.options(
                joinedload(Order.pickup_location),
                selectinload(Order.items),
            ).all()

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
                # Build response dict from order attributes
                order_dict = {
                    "id": order.id,
                    "uuid": order.uuid,
                    "order_id": order.order_id,
                    "order_date": order.order_date,
                    "channel": order.channel,
                    "client_id": order.client_id,
                    # Consignee
                    "consignee_full_name": order.consignee_full_name,
                    "consignee_phone": order.consignee_phone,
                    "consignee_email": order.consignee_email,
                    "consignee_address": order.consignee_address,
                    "consignee_pincode": order.consignee_pincode,
                    "consignee_city": order.consignee_city,
                    "consignee_state": order.consignee_state,
                    "consignee_country": order.consignee_country,
                    # Payment
                    "payment_mode": order.payment_mode,
                    "total_amount": float(order.total_amount or 0),
                    "order_value": float(order.order_value or 0),
                    "cod_to_collect": float(order.cod_to_collect or 0),
                    # Package
                    "weight": float(order.weight or 0),
                    "applicable_weight": float(order.applicable_weight or 0),
                    "volumetric_weight": float(order.volumetric_weight or 0),
                    "length": float(order.length or 0),
                    "breadth": float(order.breadth or 0),
                    "height": float(order.height or 0),
                    # Shipment
                    "status": order.status,
                    "sub_status": order.sub_status,
                    "awb_number": order.awb_number,
                    "courier_partner": order.courier_partner,
                    "zone": order.zone,
                    # Pickup
                    "pickup_location_code": order.pickup_location_code,
                    "pickup_location": serialize_pickup_location(order.pickup_location),
                    # Dates
                    "booking_date": order.booking_date,
                    "delivered_date": order.delivered_date,
                    "edd": order.edd,
                    "created_at": order.created_at,
                    # Items - convert from relationship to response format
                    "items": [
                        OrderItemResponse(
                            id=item.id,
                            name=item.name,
                            sku_code=item.sku_code,
                            quantity=item.quantity,
                            unit_price=float(item.unit_price or 0),
                        )
                        for item in (order.items or [])
                        if not getattr(item, "is_deleted", False)
                    ],
                }

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
    def get_previous_orders(order_id: str, page_number: int = 1, batch_size: int = 10):
        """
        Get previous orders for the same phone number as the given order ID with pagination
        """
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                # First, get the current order to extract the phone number
                current_order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == order_id,
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
                    Order.client_id == client_id,
                    Order.is_deleted == False,
                    Order.order_id != order_id,  # Exclude current order
                )

                # Get status counts for previous orders
                status_count_query = (
                    db.query(Order.status, func.count(Order.id))
                    .filter(
                        Order.consignee_phone == current_order.consignee_phone,
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
                    base_query.options(
                        joinedload(Order.pickup_location),
                        selectinload(Order.items),
                    )
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
                    order_dict = {
                        "id": order.id,
                        "uuid": order.uuid,
                        "order_id": order.order_id,
                        "order_date": order.order_date,
                        "channel": order.channel,
                        "client_id": order.client_id,
                        # Consignee
                        "consignee_full_name": order.consignee_full_name,
                        "consignee_phone": order.consignee_phone,
                        "consignee_email": order.consignee_email,
                        "consignee_address": order.consignee_address,
                        "consignee_pincode": order.consignee_pincode,
                        "consignee_city": order.consignee_city,
                        "consignee_state": order.consignee_state,
                        "consignee_country": order.consignee_country,
                        # Payment
                        "payment_mode": order.payment_mode,
                        "total_amount": float(order.total_amount or 0),
                        "order_value": float(order.order_value or 0),
                        "cod_to_collect": float(order.cod_to_collect or 0),
                        # Package
                        "weight": float(order.weight or 0),
                        "applicable_weight": float(order.applicable_weight or 0),
                        "volumetric_weight": float(order.volumetric_weight or 0),
                        "length": float(order.length or 0),
                        "breadth": float(order.breadth or 0),
                        "height": float(order.height or 0),
                        # Shipment
                        "status": order.status,
                        "sub_status": order.sub_status,
                        "awb_number": order.awb_number,
                        "courier_partner": order.courier_partner,
                        "zone": order.zone,
                        # Pickup
                        "pickup_location_code": order.pickup_location_code,
                        "pickup_location": serialize_pickup_location(
                            order.pickup_location
                        ),
                        # Dates
                        "booking_date": order.booking_date,
                        "delivered_date": order.delivered_date,
                        "edd": order.edd,
                        "created_at": order.created_at,
                        # Items - convert from relationship to response format
                        "items": [
                            OrderItemResponse(
                                id=item.id,
                                name=item.name,
                                sku_code=item.sku_code,
                                quantity=item.quantity,
                                unit_price=float(item.unit_price or 0),
                            )
                            for item in (order.items or [])
                            if not getattr(item, "is_deleted", False)
                        ],
                    }
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

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # query the db for fetching the latest 3 orders with distinct addresses

            customers = (
                db.query(Order)
                .options(selectinload(Order.items))
                .filter(
                    Order.client_id == client_id,
                    Order.consignee_phone == phone,
                )
                .distinct(Order.consignee_address)
                .order_by(asc(Order.consignee_address), desc(Order.created_at))
                .limit(3)
                .all()
            )

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

            query = db.query(Order).options(selectinload(Order.items))

            # # applying company and client filter
            fetched_orders = query.filter(
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
                        Order.client_id == client_id,
                        Order.order_id == order_id,
                        Order.is_deleted == False,
                    )
                    .options(
                        joinedload(Order.pickup_location),
                        selectinload(Order.items),
                        selectinload(Order.tracking_events),
                        selectinload(Order.audit_logs),
                        joinedload(Order.billing),
                    )
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

                # TODO: This legacy WooCommerce integration needs to be updated
                # to use client_id instead of company_id (which was removed from Order model)
                # For now, this method is not functional
                existing_order = (
                    db.query(Order)
                    .filter(
                        # Order.company_id == str(request["com_id"]),  # REMOVED - company_id no longer exists
                        Order.order_id
                        == str(request["id"]),
                    )
                    .first()
                )

                from models.order_audit_log import OrderAuditLog

                if existing_order:
                    # If the order exists, update its attributes
                    # Filter out products as it's stored in separate table
                    for key, value in newOrder.items():
                        if key != "products" and hasattr(existing_order, key):
                            setattr(existing_order, key, value)

                    # Add audit log entry
                    audit_log = OrderAuditLog.log_order_updated(
                        order_id=existing_order.id,
                        user_id=None,
                        user_name="WooCommerce Sync",
                    )
                    db.add(audit_log)

                    # Commit the changes to the database
                    db.commit()
                    return {
                        "code": 200,
                        "message": "Order Updated",
                    }  # Return the updated order
                else:
                    # If the order does not exist, create a new one
                    # Remove products from order data (stored in separate table)
                    order_data_for_db = {
                        k: v for k, v in newOrder.items() if k != "products"
                    }
                    final = Order(**{**order_data_for_db, "status": "New"})

                    db.add(final)
                    db.flush()

                    # Add audit log entry
                    audit_log = OrderAuditLog.log_order_created(
                        order_id=final.id,
                        user_id=None,
                        user_name="WooCommerce Sync",
                        source="woocommerce",
                    )
                    db.add(audit_log)

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
