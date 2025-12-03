import http
from sqlalchemy import or_, desc, cast, func
import pandas as pd
from fastapi import Response
from psycopg2 import DatabaseError
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import joinedload
from sqlalchemy.types import DateTime
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Any
from sqlalchemy import select
from io import BytesIO
import json
import base64
import pytz
import openpyxl
from openpyxl.workbook import Workbook
import io
import os.path
import time
from sqlalchemy import (
    or_,
    desc,
    cast,
    func,
    text,
    and_,
)
from sqlalchemy.types import DateTime, String
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
    Get_Order_Usging_AWB_OR_Order_Id,
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
    def create_order(
        order_data: Order_create_request_model,
    ):

        from modules.shipment.shipment_service import ShipmentService

        try:
            print(order_data)
            with get_db_session() as db:

                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                order = (
                    db.query(Return_Order)
                    .filter(
                        Return_Order.order_id == order_data.order_id,
                        Return_Order.company_id == company_id,
                        Return_Order.client_id == client_id,
                    )
                    .first()
                )

                # Throw an error if an order id for that client already exists
                if order:

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

                zone = zone_data.data["zone"]
                order_data["zone"] = zone

                order_model_instance = Return_Order.create_db_entity(order_data)

                created_order = Return_Order.create_new_order(order_model_instance)

                db.commit()

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

    @staticmethod
    async def get_all_return_orders(order_filters: Order_filters):

        # -----------------------
        # FIX: asyncpg datetime handling
        # -----------------------
        def to_naive(dt):
            if dt is None:
                return None
            return dt.astimezone(timezone.utc).replace(tzinfo=None)

        db: AsyncSession = get_db_session()

        try:
            # Extract filters
            page_number = order_filters.page_number
            batch_size = order_filters.batch_size
            order_status = order_filters.order_status
            current_status = ""
            search_term = order_filters.search_term

            # Convert dates to naive
            start_date = to_naive(order_filters.start_date)
            end_date = to_naive(order_filters.end_date)

            date_type = "order date"
            tags = ""
            repeat_customer = ""
            payment_mode = ""
            courier_filter = ""
            sku_codes = ""
            product_name = ""
            product_quantity = ""
            order_id = ""
            pincode = ""
            pickup_location = ""

            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id

            # --------------------------------
            # BASE FILTERS
            # --------------------------------
            base_filters = [
                Return_Order.company_id == company_id,
                Return_Order.client_id == client_id,
                Return_Order.is_deleted == False,
            ]

            common_filters = []
            remaining_filters = []
            sku_params = {}

            # --------------------------------
            # SEARCH FILTER
            # --------------------------------
            if search_term:
                terms = [t.strip() for t in search_term.split(",")]
                common_filters.append(
                    or_(
                        *[
                            or_(
                                Return_Order.order_id == t,
                                Return_Order.awb_number == t,
                                Return_Order.consignee_phone == t,
                                Return_Order.consignee_alternate_phone == t,
                                Return_Order.consignee_email == t,
                            )
                            for t in terms
                        ]
                    )
                )

            # --------------------------------
            # DATE FILTERS
            # --------------------------------
            if date_type == "order date":
                if start_date:
                    common_filters.append(
                        cast(Return_Order.order_date, DateTime) >= start_date
                    )
                if end_date:
                    common_filters.append(
                        cast(Return_Order.order_date, DateTime) <= end_date
                    )

            elif date_type == "booking date":
                if start_date:
                    common_filters.append(
                        cast(Return_Order.booking_date, DateTime) >= start_date
                    )
                if end_date:
                    common_filters.append(
                        cast(Return_Order.booking_date, DateTime) <= end_date
                    )

            # --------------------------------
            # CURRENT STATUS
            # --------------------------------
            if current_status:
                remaining_filters.append(Return_Order.sub_status == current_status)

            # --------------------------------
            # SKU FILTER
            # --------------------------------
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

            # --------------------------------
            # PRODUCT NAME
            # --------------------------------
            if product_name:
                names = [x.strip() for x in product_name.split(",")]
                remaining_filters.append(
                    or_(
                        *[
                            cast(Return_Order.products, String).ilike(
                                f'%"name": "%{n}%"%'
                            )
                            for n in names
                        ]
                    )
                )

            # PINCODE
            if pincode:
                pins = [x.strip() for x in pincode.split(",")]
                remaining_filters.append(
                    or_(*[Return_Order.consignee_pincode == p for p in pins])
                )

            # PICKUP LOCATION
            if pickup_location:
                remaining_filters.append(
                    Return_Order.pickup_location_code == pickup_location
                )

            # ORDER ID
            if order_id:
                ids = [x.strip() for x in order_id.split(",")]
                remaining_filters.append(Return_Order.order_id.in_(ids))

            # PAYMENT MODE
            if payment_mode:
                remaining_filters.append(Return_Order.payment_mode == payment_mode)

            # COURIER
            if courier_filter:
                remaining_filters.append(Return_Order.courier_partner == courier_filter)

            # PRODUCT QTY
            if product_quantity:
                remaining_filters.append(
                    Return_Order.product_quantity == product_quantity
                )

            # TAGS
            if tags:
                remaining_filters.append(
                    cast(Return_Order.order_tags, String).ilike(f"%{tags}%")
                )

            # --------------------------------
            # REPEAT CUSTOMER LOGIC
            # --------------------------------
            if repeat_customer is True:
                repeat_query = (
                    select(Return_Order.consignee_phone)
                    .where(
                        Return_Order.company_id == company_id,
                        Return_Order.client_id == client_id,
                        Return_Order.is_deleted == False,
                        Return_Order.consignee_phone.isnot(None),
                        Return_Order.consignee_phone != "",
                    )
                    .group_by(Return_Order.consignee_phone)
                    .having(func.count(Return_Order.id) > 1)
                )
                result = await db.execute(repeat_query)
                phones = result.scalars().all()
                remaining_filters.append(Return_Order.consignee_phone.in_(phones))

            # --------------------------------
            # STATUS COUNTS
            # --------------------------------
            status_count_query = (
                select(Return_Order.status, func.count(Return_Order.id))
                .where(*base_filters)
                .where(*common_filters)
                .where(*remaining_filters)
                .group_by(Return_Order.status)
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
                select(Return_Order)
                .options(joinedload(Return_Order.pickup_location))
                .where(*base_filters, *common_filters, *remaining_filters)
                .order_by(
                    desc(Return_Order.order_date),
                    desc(Return_Order.created_at),
                    desc(Return_Order.id),
                )
            )

            if sku_codes:
                main_query = main_query.params(**sku_params)

            if order_status != "all":
                main_query = main_query.where(Return_Order.status == order_status)

            # --------------------------------
            # TOTAL COUNT
            # --------------------------------
            count_query = select(func.count()).select_from(
                select(Return_Order.id)
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
                    select(Return_Order.consignee_phone, func.count(Return_Order.id))
                    .where(
                        Return_Order.company_id == company_id,
                        Return_Order.client_id == client_id,
                        Return_Order.is_deleted == False,
                        Return_Order.consignee_phone.in_(phone_numbers),
                    )
                    .group_by(Return_Order.consignee_phone)
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
                message="Return orders fetched Successfully trigger",
                status=True,
                data={
                    "orders": orders_response,
                    "total_count": total_count,
                    "status_counts": status_counts,
                },
            )

        except Exception as e:
            logger.error(f"Unhandled error (Return Orders): {e}")
            return GenericResponseModel(
                status_code=500,
                status=False,
                message="An internal server error occurred.",
                data=str(e),
            )

        finally:
            if db:
                await db.close()

    @staticmethod
    async def Get_Order_Using_Awb_OR_OrderId(
        get_Order_Usging_AWB_OR_Order_Id: Get_Order_Usging_AWB_OR_Order_Id,
    ):
        db: AsyncSession = None
        try:
            db = get_db_session()  # async session (no await)
            print(1)
            client_id = context_user_data.get().client_id

            # ------------------------------
            # Build Query With joinedload
            # ------------------------------
            query = (
                select(Order)
                .where(Order.client_id == client_id)
                .options(
                    joinedload(Order.pickup_location),  # <-- load pickup_location
                )
            )
            print(2)

            # apply filter
            if get_Order_Usging_AWB_OR_Order_Id.type == "orderid":
                query = query.where(
                    Order.order_id == get_Order_Usging_AWB_OR_Order_Id.items
                )
            else:
                query = query.where(
                    Order.awb_number == get_Order_Usging_AWB_OR_Order_Id.items
                )
            print(3)
            # execute
            result = await db.execute(query)
            order_obj = result.scalars().first()  # <-- only ONE
            print(4)
            if not order_obj:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Order not found",
                    status=False,
                )

            # ------------------------------
            # Convert ORM → Schema Model
            # ------------------------------
            order_dict = order_obj.to_model().model_dump()

            # Your Pydantic response model
            response_data = Single_Order_Response_Model(**order_dict).model_dump()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Order fetched successfully",
                data=response_data,
            )

        except Exception as e:
            logger.error(f"Error fetching order: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An unexpected error occurred. Please try again later.",
                data=str(e),
            )

        finally:
            if db:
                await db.close()

    @staticmethod
    async def dev_create_return_order(
        order_data: Order_create_request_model,
    ):
        db: AsyncSession = None
        try:
            # Print input data safely
            print("Development return order creation =>", jsonable_encoder(order_data))
            company_id = context_user_data.get().company_id
            client_id = context_user_data.get().client_id
            # return False
            # Remove unnecessary fields
            order_data_dict = order_data.model_dump()
            order_data_dict.pop("courier", None)
            order_data_dict.pop("return_reason", None)
            # Acquire AsyncSession manually
            db = get_db_session()  # <-- Correct: no 'await'

            # Check if return order already exists
            query = select(Return_Order).where(
                Return_Order.order_id == order_data.order_id,
                Return_Order.client_id == client_id,
            )
            result = await db.execute(query)
            existing_return_order = result.scalars().first()

            if existing_return_order:
                if existing_return_order.status not in ["new", "cancelled"]:
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

                # if courier_id is not None and existing_return_order.status == "new":
                #     from modules.shipment.shipment_service import ShipmentService

                #     shipmentResponse = await ShipmentService.dev_assign_return_awb(
                #         return_order=existing_return_order,
                #         courier_id=courier_id,
                #     )
                #     return shipmentResponse

                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    data={"order_id": order_data.order_id},
                    message="Return Order Id already exists",
                )

            # DEV-specific field mapping
            pickup_fields_mapping = {
                "pickup_full_name": "consignee_full_name",
                "pickup_phone": "consignee_phone",
                "pickup_alternate_phone": "consignee_alternate_phone",
                "pickup_email": "consignee_email",
                "pickup_company": "consignee_company",
                "pickup_gstin": "consignee_gstin",
                "pickup_address": "consignee_address",
                "pickup_landmark": "consignee_landmark",
                "pickup_city": "consignee_city",
                "pickup_state": "consignee_state",
                "pickup_pincode": "consignee_pincode",
                "pickup_country": "consignee_country",
                "return_location_code": "pickup_location_code",
            }
            for src, dest in pickup_fields_mapping.items():
                if src in order_data_dict:
                    order_data_dict[dest] = order_data_dict.pop(src)

            billing_fields = [
                "billing_full_name",
                "billing_phone",
                "billing_email",
                "billing_address",
                "billing_landmark",
                "billing_pincode",
                "billing_city",
                "billing_state",
                "billing_country",
            ]
            order_data_dict["billing_is_same_as_consignee"] = True
            for field in billing_fields:
                order_data_dict[field] = None

            order_data_dict["client_id"] = client_id
            order_data_dict["company_id"] = company_id
            order_data_dict["order_type"] = "B2C"

            # Weight calculations
            volumetric_weight = round(
                order_data_dict["length"]
                * order_data_dict["breadth"]
                * order_data_dict["height"]
                / 5000,
                3,
            )
            order_data_dict["volumetric_weight"] = volumetric_weight
            order_data_dict["applicable_weight"] = round(
                max(order_data_dict["weight"], volumetric_weight), 3
            )
            order_data_dict["status"] = "new"
            order_data_dict["sub_status"] = "new"

            order_data_dict["action_history"] = [
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Return Order Created on Platform",
                    "user_data": context_user_data.get().id,
                }
            ]
            order_data_dict["order_date"] = datetime.now(timezone.utc)

            now = datetime.now(timezone.utc)
            order_data_dict["booking_date"] = now

            order_data_dict["product_quantity"] = sum(
                product["quantity"] for product in order_data_dict["products"]
            )

            # Pickup location pincode
            pickup_location_pincode = None
            if "pickup_location_code" in order_data_dict:
                query = select(Pickup_Location.pincode).where(
                    Pickup_Location.location_code
                    == order_data_dict["pickup_location_code"]
                )
                result = await db.execute(query)
                pickup_location_pincode = result.scalar_one_or_none()

            if pickup_location_pincode is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid Pickup Location",
                )

            # Zone calculation
            delivery_pincode_for_zone = order_data_dict.get("consignee_pincode")
            if delivery_pincode_for_zone:
                from modules.shipment.shipment_service import ShipmentService

                zone_data = await ShipmentService.calculate_shipping_zone(
                    pickup_location_pincode, delivery_pincode_for_zone
                )
                order_data_dict["zone"] = (
                    zone_data.data["zone"] if zone_data.status else "Unknown"
                )
            else:
                order_data_dict["zone"] = "Unknown"
            print("READY TO INSERT INTO RETURN PANEL PAGE>>>>>>>>")
            print(jsonable_encoder(order_data_dict))
            print("<<<<<<<<<READY TO INSERT INTO RETURN PANEL PAGE")

            order_model_instance = Return_Order.create_db_entity(order_data_dict)
            db.add(order_model_instance)
            await db.commit()
            print(7)
            await db.refresh(order_model_instance)

            # if courier_id is not None:
            #     from modules.shipment.shipment_service import ShipmentService

            #     shipmentResponse = await ShipmentService.dev_assign_return_awb(
            #         return_order=new_return_order,
            #         courier_id=courier_id,
            #     )
            #     return shipmentResponse

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

        finally:
            if db:
                await db.close()  # Manually close AsyncSession

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

    @staticmethod
    async def export_orders(order_filters: Order_filters):
        try:
            # destructure the filters
            order_status = order_filters.order_status
            search_term = order_filters.search_term
            start_date = order_filters.start_date
            end_date = order_filters.end_date
            payment_mode = ""
            courier_filter = ""
            sku_codes = ""
            order_id = ""

            # Convert timezone aware → naive
            if start_date and start_date.tzinfo is not None:
                start_date = start_date.replace(tzinfo=None)

            if end_date and end_date.tzinfo is not None:
                end_date = end_date.replace(tzinfo=None)

            db: AsyncSession = get_db_session()
            client_id = context_user_data.get().client_id

            # BASE QUERY — CHANGED Order → Return_Order
            stmt = (
                select(Return_Order)
                .options(joinedload(Return_Order.pickup_location))
                .where(
                    and_(
                        Return_Order.client_id == client_id,
                        Return_Order.is_deleted == False,
                        cast(Return_Order.order_date, DateTime) >= start_date,
                        cast(Return_Order.order_date, DateTime) <= end_date,
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
                            Return_Order.order_id == term,
                            Return_Order.awb_number == term,
                            Return_Order.consignee_phone == term,
                            Return_Order.consignee_alternate_phone == term,
                            Return_Order.consignee_email == term,
                        )
                    )

                stmt = stmt.where(or_(*conditions))

            # SKU filter
            if sku_codes:
                sku_list = [s.strip() for s in sku_codes.split(",")]
                like_conditions = [
                    cast(Return_Order.products, String).like(f'%"sku_code": "{sku}"%')
                    for sku in sku_list
                ]
                stmt = stmt.where(or_(*like_conditions))

            # Payment mode
            if payment_mode:
                stmt = stmt.where(Return_Order.payment_mode == payment_mode)

            # Courier filter
            if courier_filter:
                stmt = stmt.where(Return_Order.courier_partner == courier_filter)

            # Status filter
            if order_status != "all":
                stmt = stmt.where(Return_Order.status == order_status)

            # Order ID filter
            if order_id:
                order_ids = [o.strip() for o in order_id.split(",")]
                stmt = stmt.where(Return_Order.order_id.in_(order_ids))

            # Ordering
            stmt = stmt.order_by(
                desc(Return_Order.order_date), desc(Return_Order.created_at)
            )

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
                    "Qc Reason": order.qc_reason,
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
                    # "pickup_completion_date": (
                    #     order.pickup_completion_date.strftime("%Y-%m-%d %H:%M:%S")
                    #     if order.pickup_completion_date
                    #     else ""
                    # ),
                    # "First Out for Pickup Date": (
                    #     order.first_ofp_date.strftime("%Y-%m-%d %H:%M:%S")
                    #     if order.first_ofp_date
                    #     else ""
                    # ),
                    # "Pickup failure reason": order.pickup_failed_reason or "",
                    # "First Out for Delivery Date": (
                    #     order.first_ofd_date.strftime("%Y-%m-%d %H:%M:%S")
                    #     if order.first_ofd_date
                    #     else ""
                    # ),
                    # "RTO Initiated Date": (
                    #     order.rto_initiated_date.strftime("%Y-%m-%d %H:%M:%S")
                    #     if order.rto_initiated_date
                    #     else ""
                    # ),
                    # "RTO Delivered Date": (
                    #     order.rto_delivered_date.strftime("%Y-%m-%d %H:%M:%S")
                    #     if order.rto_delivered_date
                    #     else ""
                    # ),
                    # "RTO Reason": order.rto_reason or "",
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
    async def delete_order(order_id: str) -> GenericResponseModel:
        try:
            async with get_db_session() as db:
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id

                # Fetch the order
                stmt = select(Return_Order).where(
                    and_(
                        Return_Order.order_id == order_id,
                        Return_Order.client_id == client_id,
                    )
                )
                result = await db.execute(stmt)
                order = result.scalar_one_or_none()
                print(order, "<<order>>")
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
    async def edit_order_fetch(
        edit_order_fetch: Get_Order_Usging_AWB_OR_Order_Id,
    ) -> GenericResponseModel:
        db: AsyncSession = None
        try:
            db = get_db_session()  # async session (no await)
            client_id = context_user_data.get().client_id
            # ------------------------------
            # Build Query With joinedload
            # ------------------------------
            query = (
                select(Return_Order)
                .where(Return_Order.client_id == client_id)
                .options(
                    joinedload(
                        Return_Order.pickup_location
                    ),  # <-- load pickup_location
                )
            )
            # apply filter
            if edit_order_fetch.type == "orderid":
                query = query.where(Return_Order.order_id == edit_order_fetch.items)
            else:
                query = query.where(Return_Order.awb_number == edit_order_fetch.items)
            # execute
            result = await db.execute(query)
            order_obj = result.scalars().first()  # <-- only ONE
            if not order_obj:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Order not found",
                    status=False,
                )
            order_dict = order_obj.to_model().model_dump()
            # Your Pydantic response model
            response_data = Single_Order_Response_Model(**order_dict).model_dump()
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Order fetched successfully",
                data=response_data,
            )
        except Exception as e:
            logger.error(f"Error fetching order: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An unexpected error occurred. Please try again later.",
                data=str(e),
            )
        finally:
            if db:
                await db.close()

    @staticmethod
    async def update_order(order_id: str, order_data: Order_create_request_model):
        try:
            async with get_db_session() as db:
                from modules.shipment.shipment_service import ShipmentService

                print("Welcome to update order service")
                company_id = context_user_data.get().company_id
                client_id = context_user_data.get().client_id
                # -------------------------------
                # FETCH EXISTING ORDER
                # -------------------------------
                stmt = select(Return_Order).where(
                    and_(
                        Return_Order.order_id == order_id,
                        Return_Order.client_id == client_id,
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
                    stmt2 = select(Return_Order).where(
                        and_(
                            Return_Order.order_id == order_data.order_id,
                            Return_Order.client_id == client_id,
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
