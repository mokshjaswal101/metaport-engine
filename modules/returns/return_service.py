import http
from sqlalchemy import or_, desc, cast, func, select
import pandas as pd
from fastapi import Response
from psycopg2 import DatabaseError
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import joinedload
from sqlalchemy.types import DateTime
from typing import List, Any
from io import BytesIO
import json
import base64
import pytz
import openpyxl
from openpyxl.workbook import Workbook
import io
import os.path
import time
from fastapi.encoders import jsonable_encoder
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, PatternFill, Font


from context_manager.context import context_user_data, get_db_session

from logger import logger

# schema
from schema.base import GenericResponseModel
from .return_schema import (
    Order_create_request_model,
    Order_Response_Model,
    Single_Order_Response_Model,
    Order_filters,
    PickupLocationResponseModel,
    Get_Order_Usging_AWB_OR_Order_Id,
    Dev_Return_Order_Create_Request_Model,
)

from shipping_partner.shiperfecto.status_mapping import status_mapping

# models
from models import Return_Order, Order, Pickup_Location


original_tz = "Asia/Kolkata"


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


class ReturnService:

    @staticmethod
    async def create_order(order_data: Order_create_request_model):

        from modules.shipment.shipment_service import ShipmentService

        try:
            print(order_data)

            async with get_db_session() as db:

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # -----------------------------
                # 1️⃣ CHECK IF ORDER ALREADY EXISTS
                # -----------------------------
                existing_order_stmt = select(Return_Order).where(
                    Return_Order.order_id == order_data.order_id,
                    Return_Order.company_id == company_id,
                    Return_Order.client_id == client_id,
                )

                existing_order = await db.execute(existing_order_stmt)
                existing_order = existing_order.scalars().first()

                if existing_order:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        data={"order_id": order_data.order_id},
                        message="Order Id already exists",
                    )

                # Convert Pydantic model → dict
                order_data = order_data.model_dump()

                # Add required fields
                order_data["client_id"] = client_id
                order_data["company_id"] = company_id
                order_data["order_type"] = "B2C"

                # -----------------------------
                # 2️⃣ VOLUMETRIC CALC
                # -----------------------------
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

                order_data["volumetric_weight"] = volumetric_weight
                order_data["applicable_weight"] = applicable_weight

                # Basic order data
                order_data["status"] = "new"
                order_data["sub_status"] = "new"

                order_data["action_history"] = [
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "message": "Order Created on Platform",
                        "user_data": context_user_data.get().id,
                    }
                ]

                # Convert order_date to UTC
                order_data["order_date"] = convert_to_utc(
                    order_date=order_data["order_date"]
                )

                # Total product quantity
                order_data["product_quantity"] = sum(
                    p["quantity"] for p in order_data["products"]
                )

                # -----------------------------
                # 3️⃣ FETCH PICKUP PINCODE (fixed async code)
                # -----------------------------
                pickup_stmt = select(Pickup_Location.pincode).where(
                    Pickup_Location.location_code == order_data["pickup_location_code"]
                )

                pickup_result = await db.execute(pickup_stmt)
                pickup_pincode = pickup_result.scalar_one_or_none()

                if pickup_pincode is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pickup Location",
                    )

                # -----------------------------
                # 4️⃣ CALCULATE SHIPPING ZONE
                # -----------------------------
                zone_data = await ShipmentService.calculate_shipping_zone(
                    pickup_pincode, order_data["consignee_pincode"]
                )

                if not zone_data.status:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pincodes",
                        status=False,
                    )

                order_data["zone"] = zone_data.data["zone"]

                # -----------------------------
                # 5️⃣ CREATE ORDER
                # -----------------------------
                order_model_instance = Return_Order.create_db_entity(order_data)

                created_order = Return_Order.create_new_order(order_model_instance)

                # IMPORTANT: Async commit
                await db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order created Successfully",
                    data={"order_id": created_order.order_id},
                    status=True,
                )

        # -----------------------------
        # 6️⃣ ERROR HANDLING
        # -----------------------------
        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error creating Order: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    # Fixed version - Add this method to the ReturnService class in return_service.py

    @staticmethod
    async def bulk_create_return_orders(order_ids: List[str]):
        """
        Create return orders for multiple forward orders in bulk
        """
        results = []
        successful = 0
        failed = 0

        # Process each order in its own try-catch to prevent one failure from affecting others
        for order_id in order_ids:
            try:
                async with get_db_session() as db:
                    company_id = context_user_data.get().company_id
                    client_id = context_user_data.get().client_id

                    # Fetch the forward order
                    forward_order_stmt = (
                        select(Order)
                        .where(
                            Order.order_id == order_id,
                            Order.company_id == company_id,
                            Order.client_id == client_id,
                        )
                        .options(joinedload(Order.pickup_location))
                    )

                    forward_order_result = await db.execute(forward_order_stmt)
                    forward_order = forward_order_result.scalars().first()

                    # Check if forward order exists
                    if not forward_order:
                        results.append(
                            {
                                "order_id": order_id,
                                "forward_order_id": order_id,
                                "status": "failed",
                                "message": "Forward order not found",
                                "return_order_id": None,
                            }
                        )
                        failed += 1
                        logger.warning(f"Forward order {order_id} not found")
                        continue

                    # Validate order status - only create returns for delivered orders
                    if forward_order.status != "delivered":
                        results.append(
                            {
                                "order_id": order_id,
                                "forward_order_id": order_id,
                                "status": "failed",
                                "message": f"Order must be delivered to create return. Current status: {forward_order.status}",
                                "return_order_id": None,
                            }
                        )
                        failed += 1
                        logger.warning(
                            f"Order {order_id} status is {forward_order.status}, not delivered"
                        )
                        continue

                    # Generate return order ID with timestamp for uniqueness
                    return_order_id = f"RET_{order_id}"

                    # Check if return order already exists for this forward order
                    existing_return_stmt = select(Return_Order).where(
                        Return_Order.order_id.like(f"RET_{order_id}%"),
                        Return_Order.company_id == company_id,
                        Return_Order.client_id == client_id,
                    )

                    existing_return_result = await db.execute(existing_return_stmt)
                    existing_return = existing_return_result.scalars().first()

                    if existing_return:
                        results.append(
                            {
                                "order_id": order_id,
                                "forward_order_id": order_id,
                                "status": "failed",
                                "message": "Return order already exists for this forward order",
                                "return_order_id": existing_return.order_id,
                            }
                        )
                        failed += 1
                        logger.warning(
                            f"Return already exists for order {order_id}: {existing_return.order_id}"
                        )
                        continue

                    # Fetch pickup location details
                    pickup_location_stmt = select(Pickup_Location).where(
                        Pickup_Location.location_code
                        == forward_order.pickup_location_code
                    )
                    pickup_result = await db.execute(pickup_location_stmt)
                    pickup_location = pickup_result.scalars().first()

                    if not pickup_location:
                        results.append(
                            {
                                "order_id": order_id,
                                "forward_order_id": order_id,
                                "status": "failed",
                                "message": f"Pickup location {forward_order.pickup_location_code} not found",
                                "return_order_id": None,
                            }
                        )
                        failed += 1
                        logger.warning(
                            f"Pickup location {forward_order.pickup_location_code} not found for order {order_id}"
                        )
                        continue

                    # Calculate order value from products
                    order_value = 0
                    if forward_order.products and isinstance(
                        forward_order.products, list
                    ):
                        order_value = sum(
                            float(product.get("unit_price", 0.0))
                            * int(product.get("quantity", 0))
                            for product in forward_order.products
                        )
                    order_value = round(order_value, 2)

                    logger.info(
                        f"Creating return order for {order_id}, order_value: {order_value}"
                    )

                    # Prepare return order data
                    return_order_data = {
                        # Order identification
                        "order_id": return_order_id,
                        "order_date": datetime.now(pytz.utc),
                        "channel": forward_order.channel or "manual",
                        # CONSIGNEE DETAILS (warehouse - where return will be delivered)
                        "consignee_full_name": pickup_location.contact_person_name
                        or "Warehouse",
                        "consignee_phone": str(
                            pickup_location.contact_person_phone or ""
                        ),
                        "consignee_alternate_phone": str(
                            pickup_location.alternate_phone or ""
                        ),
                        "consignee_email": pickup_location.contact_person_email or "",
                        # "consignee_company": pickup_location.company or "",
                        # "consignee_gstin": pickup_location.gstin or "",
                        "consignee_address": pickup_location.address or "",
                        "consignee_landmark": pickup_location.landmark or "",
                        "consignee_city": pickup_location.city or "",
                        "consignee_state": pickup_location.state or "",
                        "consignee_pincode": str(pickup_location.pincode or ""),
                        "consignee_country": pickup_location.country or "India",
                        # BILLING DETAILS (same as consignee for returns)
                        "billing_is_same_as_consignee": True,
                        "billing_full_name": None,
                        "billing_phone": None,
                        "billing_email": None,
                        "billing_address": None,
                        "billing_landmark": None,
                        "billing_pincode": None,
                        "billing_city": None,
                        "billing_state": None,
                        "billing_country": None,
                        # PICKUP DETAILS (customer location - where to pick up return from)
                        "pickup_location_code": forward_order.pickup_location_code,
                        # Package details
                        "weight": (
                            float(forward_order.weight) if forward_order.weight else 0.5
                        ),
                        "length": (
                            float(forward_order.length) if forward_order.length else 10
                        ),
                        "breadth": (
                            float(forward_order.breadth)
                            if forward_order.breadth
                            else 10
                        ),
                        "height": (
                            float(forward_order.height) if forward_order.height else 10
                        ),
                        # Products
                        "products": (
                            forward_order.products if forward_order.products else []
                        ),
                        # Payment details - returns are always prepaid
                        "payment_mode": "prepaid",
                        "shipping_charges": 0,
                        "cod_charges": 0,
                        "gift_wrap_charges": 0,
                        "other_charges": 0,
                        "discount": 0,
                        "tax_amount": 0,
                        "order_value": order_value,
                        "total_amount": order_value,
                        # System fields
                        "client_id": client_id,
                        "company_id": company_id,
                        "order_type": "B2C",
                        "status": "new",
                        "sub_status": "new",
                        # Action history
                        "action_history": [
                            {
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                "message": "Return Order Created via Bulk Action",
                                "user_data": context_user_data.get().id,
                            }
                        ],
                        # Set booking_date and delivered_date to None (since they're nullable now)
                        "booking_date": None,
                        "delivered_date": None,
                        "edd": datetime.now(pytz.utc)
                        + timedelta(days=7),  # Default EDD
                    }

                    # Calculate volumetric weight
                    volumetric_weight = round(
                        (
                            return_order_data["length"]
                            * return_order_data["breadth"]
                            * return_order_data["height"]
                        )
                        / 5000,
                        3,
                    )

                    applicable_weight = round(
                        max(return_order_data["weight"], volumetric_weight), 3
                    )

                    return_order_data["volumetric_weight"] = volumetric_weight
                    return_order_data["applicable_weight"] = applicable_weight

                    # Calculate product quantity
                    return_order_data["product_quantity"] = (
                        sum(p.get("quantity", 0) for p in return_order_data["products"])
                        if return_order_data["products"]
                        else 0
                    )

                    # Calculate shipping zone
                    try:
                        from modules.shipment.shipment_service import ShipmentService

                        zone_data = await ShipmentService.calculate_shipping_zone(
                            str(pickup_location.pincode),
                            str(forward_order.consignee_pincode),
                        )

                        if zone_data.status:
                            return_order_data["zone"] = zone_data.data["zone"]
                        else:
                            return_order_data["zone"] = "A"  # Default zone
                            logger.warning(
                                f"Could not calculate zone for order {order_id}, using default 'A'"
                            )
                    except Exception as zone_error:
                        logger.error(
                            f"Error calculating zone for order {order_id}: {str(zone_error)}"
                        )
                        return_order_data["zone"] = "A"  # Default zone

                    # Create return order entity
                    new_return_order = Return_Order(**return_order_data)
                    db.add(new_return_order)

                    # Commit this specific order
                    await db.commit()
                    await db.refresh(new_return_order)

                    logger.info(
                        f"Successfully created return order {return_order_id} for forward order {order_id}"
                    )

                    results.append(
                        {
                            "order_id": order_id,
                            "forward_order_id": order_id,
                            "status": "success",
                            "message": "Return order created successfully",
                            "return_order_id": return_order_id,
                        }
                    )
                    successful += 1

            except Exception as e:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Error creating return for order {order_id}: {str(e)}",
                )
                import traceback

                logger.error(f"Traceback: {traceback.format_exc()}")

                results.append(
                    {
                        "order_id": order_id,
                        "forward_order_id": order_id,
                        "status": "failed",
                        "message": f"Error: {str(e)}",
                        "return_order_id": None,
                    }
                )
                failed += 1

        # Return overall results
        return GenericResponseModel(
            status_code=(
                http.HTTPStatus.OK if successful > 0 else http.HTTPStatus.BAD_REQUEST
            ),
            message=f"Bulk return order creation completed: {successful} successful, {failed} failed",
            data={
                "total": len(order_ids),
                "successful": successful,
                "failed": failed,
                "results": results,
            },
            status=True if successful > 0 else False,
        )

    @staticmethod
    async def get_all_orders(order_filters: Order_filters):  # Make it async

        try:
            # destruct the filters
            page_number = order_filters.page_number
            batch_size = order_filters.batch_size
            order_status = order_filters.order_status
            search_term = order_filters.search_term
            start_date = order_filters.start_date
            end_date = order_filters.end_date
            print(order_filters.page_number, "||1||")

            db = get_db_session()

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # Build the query using select() instead of query()
            print(company_id, "||query 1 query||", client_id)
            stmt = select(Return_Order)
            print(stmt, "||query query query||")

            # applying company and client filter
            stmt = stmt.filter(
                Return_Order.company_id == company_id,
                Return_Order.client_id == client_id,
            )

            print(order_filters.page_number, "||2||")

            # if search term is present, give it the highest priority and no other filter will be applied
            if search_term != "":
                stmt = stmt.filter(
                    or_(
                        Return_Order.order_id == search_term,
                        Return_Order.awb_number == search_term,
                        Return_Order.consignee_phone == search_term,
                        Return_Order.consignee_alternate_phone == search_term,
                        Return_Order.consignee_email == search_term,
                    )
                )
                print(order_filters.page_number, "||3||")

            # add additional filters, only if search term is not there
            else:
                # date range filter
                print(order_filters.page_number, "||4||")

                stmt = stmt.filter(
                    Return_Order.order_date >= start_date,
                    Return_Order.order_date <= end_date,
                )
                print(order_filters.page_number, "||5||")

                # status filter
                if order_status != "all":
                    stmt = stmt.filter(Return_Order.status == order_status)

            # calculate the total count before the pagination filter is applied
            count_stmt = select(func.count()).select_from(stmt.subquery())
            count_result = await db.execute(count_stmt)
            total_count = count_result.scalar()
            print(order_filters.page_number, "||6||")

            # fetch the orders in descending order of order date
            stmt = stmt.order_by(
                desc(Return_Order.order_date), desc(Return_Order.created_at)
            )
            print(order_filters.page_number, "||7||")

            # pagination
            offset_value = (page_number - 1) * batch_size
            stmt = stmt.offset(offset_value).limit(batch_size)

            # Add joinedload for relationships
            stmt = stmt.options(joinedload(Return_Order.pickup_location))

            # Execute the query
            result = await db.execute(stmt)
            fetched_orders = result.scalars().all()
            print(fetched_orders, "||8||")

            fetched_orders = [
                Order_Response_Model(
                    **order.to_model().model_dump(),
                )
                for order in fetched_orders
            ]
            print(fetched_orders, "||final||")

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                data={"orders": fetched_orders, "total_count": total_count},
                status=True,
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fetching Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the Orders.",
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
    def Get_Order_Using_Awb_OR_OrderId(
        get_Order_Usging_AWB_OR_Order_Id: Get_Order_Usging_AWB_OR_Order_Id,
    ):
        try:

            with get_db_session() as db:
                # get_Order_Usging_AWB_OR_Order_Id.items
                # get_Order_Usging_AWB_OR_Order_Id.type
                client_id = context_user_data.get().client_id
                query = db.query(Order)
                if get_Order_Usging_AWB_OR_Order_Id.type == "orderid":
                    fetched_orders = query.filter(
                        Order.order_id == get_Order_Usging_AWB_OR_Order_Id.items,
                        Order.client_id == client_id,
                    )
                else:
                    fetched_orders = query.filter(
                        Order.awb_number == get_Order_Usging_AWB_OR_Order_Id.items,
                        Order.client_id == client_id,
                    )

                fetched_orders = [
                    order.to_model().model_dump() for order in fetched_orders
                ]

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Orders fetched Successfully",
                    data=fetched_orders,
                    status=True,
                )

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
    def dev_create_return_order(
        order_data: Dev_Return_Order_Create_Request_Model,
    ):
        """Development endpoint for creating return orders with shadowfax courier"""

        print("Development return order creation")
        try:
            print(order_data)

            # Validate courier - only Shadowfax (ID 24) is allowed for return orders
            courier_id = order_data.courier
            if courier_id != 24:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Invalid courier",
                )

            # Remove fields that don't belong in the database model
            order_data_dict = order_data.model_dump()
            if "courier" in order_data_dict:
                del order_data_dict["courier"]
            if "return_reason" in order_data_dict:
                del order_data_dict["return_reason"]

            with get_db_session() as db:
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # Check if return order already exists
                existing_return_order = (
                    db.query(Return_Order)
                    .filter(
                        Return_Order.order_id == order_data.order_id,
                        Return_Order.company_id == company_id,
                        Return_Order.client_id == client_id,
                    )
                    .first()
                )

                # Throw an error if return order ID already exists
                if existing_return_order:
                    if (
                        existing_return_order.status != "new"
                        and existing_return_order.status != "cancelled"
                    ):
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.CONFLICT,
                            status=False,
                            data={
                                "awb_number": existing_return_order.awb_number or "",
                                "delivery_partner": existing_return_order.courier_partner
                                or "",
                            },
                            message="Return AWB already assigned",
                        )

                    # If courier_id is provided and status is new, assign AWB
                    if courier_id is not None and existing_return_order.status == "new":
                        from modules.shipment.shipment_service import ShipmentService

                        shipmentResponse = ShipmentService.dev_assign_return_awb(
                            return_order=existing_return_order,
                            courier_id=courier_id,
                        )
                        return shipmentResponse

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        data={"order_id": order_data.order_id},
                        message="Return Order Id already exists",
                    )

                # DEV-SPECIFIC FIELD TRANSFORMATIONS FOR RETURN ORDERS
                # Map pickup_* fields from dev schema to consignee_* fields for database model
                if "pickup_full_name" in order_data_dict:
                    order_data_dict["consignee_full_name"] = order_data_dict.pop(
                        "pickup_full_name"
                    )
                if "pickup_phone" in order_data_dict:
                    order_data_dict["consignee_phone"] = order_data_dict.pop(
                        "pickup_phone"
                    )
                if "pickup_alternate_phone" in order_data_dict:
                    order_data_dict["consignee_alternate_phone"] = order_data_dict.pop(
                        "pickup_alternate_phone"
                    )
                if "pickup_email" in order_data_dict:
                    order_data_dict["consignee_email"] = order_data_dict.pop(
                        "pickup_email"
                    )
                if "pickup_company" in order_data_dict:
                    order_data_dict["consignee_company"] = order_data_dict.pop(
                        "pickup_company"
                    )
                if "pickup_gstin" in order_data_dict:
                    order_data_dict["consignee_gstin"] = order_data_dict.pop(
                        "pickup_gstin"
                    )
                if "pickup_address" in order_data_dict:
                    order_data_dict["consignee_address"] = order_data_dict.pop(
                        "pickup_address"
                    )
                if "pickup_landmark" in order_data_dict:
                    order_data_dict["consignee_landmark"] = order_data_dict.pop(
                        "pickup_landmark"
                    )
                if "pickup_city" in order_data_dict:
                    order_data_dict["consignee_city"] = order_data_dict.pop(
                        "pickup_city"
                    )
                if "pickup_state" in order_data_dict:
                    order_data_dict["consignee_state"] = order_data_dict.pop(
                        "pickup_state"
                    )
                if "pickup_pincode" in order_data_dict:
                    order_data_dict["consignee_pincode"] = order_data_dict.pop(
                        "pickup_pincode"
                    )
                if "pickup_country" in order_data_dict:
                    order_data_dict["consignee_country"] = order_data_dict.pop(
                        "pickup_country"
                    )

                # Map return_location_code to pickup_location_code for database model
                if "return_location_code" in order_data_dict:
                    order_data_dict["pickup_location_code"] = order_data_dict.pop(
                        "return_location_code"
                    )

                # Add default billing fields since we removed billing_details from schema
                order_data_dict["billing_is_same_as_consignee"] = True
                order_data_dict["billing_full_name"] = None
                order_data_dict["billing_phone"] = None
                order_data_dict["billing_email"] = None
                order_data_dict["billing_address"] = None
                order_data_dict["billing_landmark"] = None
                order_data_dict["billing_pincode"] = None
                order_data_dict["billing_city"] = None
                order_data_dict["billing_state"] = None
                order_data_dict["billing_country"] = None

                # Payment mode is already set to "prepaid" in the schema default                # Add company and client id to the order
                order_data_dict["client_id"] = client_id
                order_data_dict["company_id"] = company_id
                order_data_dict["order_type"] = "B2C"

                # Calculate volumetric weight
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

                order_data_dict["applicable_weight"] = applicable_weight
                order_data_dict["volumetric_weight"] = volumetric_weight
                order_data_dict["status"] = "new"
                order_data_dict["sub_status"] = "new"

                order_data_dict["action_history"] = [
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "message": "Return Order Created on Platform",
                        "user_data": context_user_data.get().id,
                    }
                ]

                # Convert to UTC
                order_data_dict["order_date"] = convert_to_utc(
                    order_date=order_data_dict["order_date"]
                )

                # calc product quantity
                order_data_dict["product_quantity"] = sum(
                    product["quantity"] for product in order_data_dict["products"]
                )

                # Fetch the pickup location pincode (now using pickup_location_code after field mapping)
                pickup_location_pincode = None
                if "pickup_location_code" in order_data_dict:
                    pickup_location_pincode = (
                        db.query(Pickup_Location.pincode)
                        .filter(
                            Pickup_Location.location_code
                            == order_data_dict["pickup_location_code"]
                        )
                        .first()
                    )
                    if pickup_location_pincode:
                        pickup_location_pincode = pickup_location_pincode[0]

                if pickup_location_pincode is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Pickup Location",
                    )

                # For return orders, calculate zone using pickup location as pickup and consignee location as delivery
                pickup_pincode_for_zone = pickup_location_pincode
                delivery_pincode_for_zone = order_data_dict.get(
                    "consignee_pincode"
                )  # This is now mapped from pickup_pincode in the dev schema

                if delivery_pincode_for_zone:
                    from modules.shipment.shipment_service import ShipmentService

                    zone_data = ShipmentService.calculate_shipping_zone(
                        pickup_pincode_for_zone, delivery_pincode_for_zone
                    )

                    if zone_data.status:
                        order_data_dict["zone"] = zone_data.data["zone"]
                    else:
                        order_data_dict["zone"] = "Unknown"
                else:
                    order_data_dict["zone"] = "Unknown"

                # Create and save return order
                new_return_order = Return_Order(**order_data_dict)
                db.add(new_return_order)
                db.commit()
                db.refresh(new_return_order)

                # If courier_id is provided, assign AWB immediately
                if courier_id is not None:
                    from modules.shipment.shipment_service import ShipmentService

                    shipmentResponse = ShipmentService.dev_assign_return_awb(
                        return_order=new_return_order,
                        courier_id=courier_id,
                    )
                    return shipmentResponse

                return GenericResponseModel(
                    status_code=http.HTTPStatus.CREATED,
                    data={"return_order_id": order_data.order_id},
                    message="Return order created successfully",
                )

        except Exception as e:
            logger.error(f"Error in dev_create_return_order: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the return order.",
            )

    @staticmethod
    def dev_cancel_return_awbs():
        """Development endpoint for canceling return AWBs"""
        import time

        time.sleep(3)
        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="Return AWBs  cancelled successfully",
        )
