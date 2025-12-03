import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
from datetime import datetime
import base64
import unicodedata
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
import pytz
import httpx
from datetime import datetime, timedelta, timezone

from utils.datetime import parse_datetime


from concurrent.futures import ThreadPoolExecutor
import http

executor = ThreadPoolExecutor()

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re

# models
from models import Pickup_Location, Order, Qc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# data
from .status_mapping import status_mapping

# service
from modules.wallet.wallet_service import WalletService


def clean_text(text):
    if text is None:
        return ""
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
    # Replace all special characters except comma and hyphen with a space
    text = re.sub(r"[^a-zA-Z0-9\s,-]", " ", text)
    # Replace multiple spaces with a single space
    return re.sub(r"\s+", " ", text).strip()


class TempModel(BaseModel):
    client_id: int


class Shadowfax:

    def date_formatter(timestamp):
        try:
            # Handle Zulu time (UTC with 'Z' at end)
            if timestamp.endswith("Z"):
                timestamp = timestamp.replace("Z", "+00:00")

            # Parse the ISO 8601 format with timezone info
            dt_object = datetime.fromisoformat(timestamp)

            # Convert to IST (UTC+5:30)
            ist_offset = timedelta(hours=5, minutes=30)
            ist_time = dt_object.astimezone(timezone(ist_offset))

            # Return formatted string
            return ist_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return f"Error: {e}"

    @staticmethod
    def create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):

        try:

            client_id = context_user_data.get().client_id

            token = credentials.get("token", None)

            if not token:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    status=False,
                    message="Invalid Credentials",
                )

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == order.pickup_location_code,
                )
                .first()
            )

            # print("token", token)

            body = {
                "order_type": "marketplace",
                "order_details": {
                    "client_order_id": "LM/"
                    + str(client_id)
                    + "/"
                    + order.order_id
                    + (f"/{str(order.cancel_count)}" if order.cancel_count > 0 else ""),
                    "actual_weight": float(order.weight) * 1000,
                    "volumetric_weight": float(order.volumetric_weight) * 1000,
                    "product_value": float(order.order_value),
                    "payment_mode": (
                        "Prepaid" if order.payment_mode.lower() == "prepaid" else "COD"
                    ),
                    "cod_amount": (
                        0
                        if order.payment_mode.lower() == "prepaid"
                        else float(order.total_amount)
                    ),
                    "promised_delivery_date": "2018-01-09T00:00:00.000Z",
                    "total_amount": float(order.total_amount),
                },
                "customer_details": {
                    "name": order.consignee_full_name,
                    "contact": order.consignee_phone,
                    "address_line_1": order.consignee_address,
                    "address_line_2": order.consignee_landmark,
                    "city": order.consignee_city,
                    "state": order.consignee_state,
                    "pincode": order.consignee_pincode,
                    "alternate_contact": order.consignee_alternate_phone,
                },
                "pickup_details": {
                    "name": pickup_location.contact_person_name,
                    "contact": pickup_location.contact_person_phone,
                    "address_line_1": pickup_location.address,
                    "address_line_2": pickup_location.landmark,
                    "city": pickup_location.city,
                    "state": pickup_location.state,
                    "pincode": pickup_location.pincode,
                    "unique_code": pickup_location.location_code,
                },
                "rts_details": {
                    "name": pickup_location.contact_person_name,
                    "contact": pickup_location.contact_person_phone,
                    "address_line_1": pickup_location.address,
                    "address_line_2": pickup_location.landmark,
                    "city": pickup_location.city,
                    "state": pickup_location.state,
                    "pincode": pickup_location.pincode,
                    "unique_code": pickup_location.location_code,
                },
                "product_details": [
                    {
                        "sku_name": product["name"],
                        "price": product["unit_price"],
                    }
                    for product in order.products
                ],
            }
            # return

            headers = {
                "Authorization": "Token " + token,
                "Content-Type": "application/json",
            }

            api_url = "https://dale.shadowfax.in/api/v3/clients/orders/"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=60
            )

            try:
                response_data = response.json()
                print("api_respose", response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response_data["message"] == "Failure":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=(
                        ", ".join(response_data["errors"])
                        if isinstance(response_data["errors"], list)
                        else str(response_data["errors"])
                    ),
                )

            # if order created successfully at shiperfecto

            response_data = response_data.get("data", None)

            if response_data == None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Failed to create shipment - data",
                )

            awb_number = response_data.get("awb_number", None)

            if awb_number == None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Failed to create shipment - awb",
                )

            # update status
            order.status = "booked"
            order.sub_status = "shipment booked"
            order.courier_status = "BOOKED"

            order.awb_number = awb_number
            order.aggregator = "shadowfax"
            order.shipping_partner_order_id = str(response_data["id"])
            order.courier_partner = delivery_partner.slug

            new_activity = {
                "event": "Shipment Created",
                "subinfo": "delivery partner - " + delivery_partner.slug,
                "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            }

            # update the activity

            order.action_history.append(new_activity)

            db.add(order)
            db.flush()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": awb_number,
                    "delivery_partner": delivery_partner.slug,
                },
                message="AWB assigned successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error posting shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while posting the shipment.",
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
    async def create_reverse_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):
        async with get_db_session() as db:
            try:
                client_id = context_user_data.get().client_id
                token = "7186514310bef086c7e48f8abd482537acc4b553"
                if not token:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.UNAUTHORIZED,
                        status=False,
                        message="Invalid Credentials",
                    )
                # ----------------------------------------------------------------------
                # 1. Fetch Pickup Location
                # ----------------------------------------------------------------------
                result = await db.execute(
                    select(Pickup_Location).where(
                        Pickup_Location.client_id == client_id,
                        Pickup_Location.location_code == order.pickup_location_code,
                    )
                )
                pickup_location = result.scalar_one_or_none()
                if not pickup_location:
                    return GenericResponseModel(
                        status=False,
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Pickup location not found",
                    )
                # ----------------------------------------------------------------------
                # 2. Build SKUs With Conditional QC
                # ----------------------------------------------------------------------
                skus_attributes = []
                for p in order.products:
                    sku = {
                        "name": p["name"],
                        "client_sku_id": p["sku_code"],
                        "price": p["unit_price"],
                        "qc_required": False,
                        "invoice_id": f"LM/{client_id}/{order.order_id}",
                    }
                    # ADD QC DETAILS ONLY IF order.qc_reason EXISTS
                    if order.qc_reason:
                        qc_result = await db.execute(
                            select(
                                Qc.category,
                                Qc.reason_name,
                                Qc.parameters_value,
                                Qc.parameters_name,
                                Qc.is_mandatory,
                            ).where(
                                Qc.client_id == client_id,
                                Qc.reason_name == order.qc_reason,
                            )
                        )
                        qc_list = qc_result.all()
                        # Cross-check: Only run if qc_list has values
                        if qc_list:
                            qc_rules = [
                                {
                                    "question": r.parameters_name,
                                    "value": r.parameters_value,
                                    "is_mandatory": r.is_mandatory,
                                }
                                for r in qc_list
                            ]
                            sku.update(
                                {
                                    "category": qc_list[0].category,
                                    "return_reason": order.qc_reason,
                                    "qc_required": "true",
                                    "qc_rules": qc_rules,
                                }
                            )
                        else:
                            sku.update(
                                {
                                    "qc_required": "false",
                                    "qc_rules": [],
                                }
                            )
                    skus_attributes.append(sku)

                # ----------------------------------------------------------------------
                # 3. Create Final Payload
                # ----------------------------------------------------------------------
                body = {
                    "client_order_number": f"LM/{client_id}/{order.order_id}",
                    "total_amount": float(order.total_amount),
                    "price": float(order.order_value),
                    "eway_bill": "",
                    "address_attributes": {
                        "address_line": f"{order.consignee_address} {order.consignee_landmark}",
                        "city": order.consignee_city,
                        "country": "India",
                        "pincode": order.consignee_pincode,
                        "name": order.consignee_full_name,
                        "phone_number": order.consignee_phone,
                        "alternate_contact": order.consignee_alternate_phone,
                    },
                    "seller_attributes": {
                        "name": pickup_location.contact_person_name,
                        "address_line": f"{pickup_location.address} {pickup_location.landmark}",
                        "city": pickup_location.city,
                        "email": pickup_location.contact_person_email,
                        "pincode": pickup_location.pincode,
                        "phone": pickup_location.contact_person_phone,
                        "unique_code": pickup_location.location_code,
                    },
                    "skus_attributes": skus_attributes,
                }

                headers = {
                    "Authorization": f"Token {token}",
                    "Content-Type": "application/json",
                }

                api_url = "https://dale.shadowfax.in/api/v3/clients/requests"
                # ----------------------------------------------------------------------
                # 4. Call API
                # ----------------------------------------------------------------------
                async with httpx.AsyncClient(timeout=60) as client:
                    response = await client.post(api_url, json=body, headers=headers)

                response_data = response.json()

                if response_data.get("message") in ["Failure", "FAILED"]:
                    errors = response_data.get("errors", "Unknown Error")
                    if isinstance(errors, list):
                        errors = ", ".join(errors)

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=str(errors),
                    )

                awb_number = response_data.get("awb_number")
                if not awb_number:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Failed to create shipment - AWB missing",
                    )

                # ----------------------------------------------------------------------
                # 5. Update DB
                # ----------------------------------------------------------------------
                order.status = "pickup"
                order.sub_status = "pickup pending"
                order.courier_status = "BOOKED"
                order.awb_number = awb_number
                order.aggregator = "shadowfax"
                order.shipping_partner_order_id = str(
                    response_data["client_request_id"]
                )
                order.courier_partner = delivery_partner.slug
                print("trigger")
                order.action_history.append(
                    {
                        "event": "Shipment Created",
                        "subinfo": f"delivery partner - {delivery_partner.slug}",
                        "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    }
                )
                print("trigger2")
                db.add(order)
                await db.commit()
                print("trigger3")
                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="AWB assigned successfully",
                    data={
                        "awb_number": awb_number,
                        "delivery_partner": delivery_partner.slug,
                    },
                )

            except Exception as e:
                logger.error(
                    f"Unhandled error: {str(e)}", extra=context_user_data.get()
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="An internal server error occurred.",
                )

            finally:
                await db.close()

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str, credentials=None):

        try:

            api_url = (
                "https://dale.shadowfax.in/api/v4/clients/orders/"
                + awb_number
                + "/track/?format=json"
            )

            print(api_url)

            headers = {
                "Authorization": "Token " + "7186514310bef086c7e48f8abd482537acc4b553",
                "Content-Type": "application/json",
            }

            response = requests.get(api_url, headers=headers, verify=False, timeout=60)

            print(response.text)

            # If tracking failed at Shiperfecto, return message
            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # If tracking failed, return message
            if response_data["message"] != "Success":

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Could not track AWB",
                )

            courier_status = response_data["order_details"]["status"]
            updated_awb_number = response_data["order_details"]["awb_number"]

            activites = response_data.get("tracking_details", "")

            db = get_db_session()

            print(courier_status)

            # update the order status, and awb if different
            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["sub_status"]
            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            # update the tracking info
            if activites:

                new_tracking_info = [
                    {
                        "status": status_mapping[activity.get("status_id")][
                            "sub_status"
                        ],
                        "description": activity.get("status", ""),
                        "subinfo": activity.get("status", ""),
                        "datetime": parse_datetime(
                            Shadowfax.date_formatter(activity.get("created"))
                        ).strftime("%d-%m-%Y %H:%M:%S"),
                        "location": activity.get("location", ""),
                    }
                    for activity in activites
                ]

                new_tracking_info.reverse()

                order.tracking_info = new_tracking_info

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": updated_awb_number,
                    "current_status": status_mapping[courier_status]["status"],
                },
                message="Tracking successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
            )

    @staticmethod
    async def cancel_shipment(order: Order_Model, awb_number: str):
        try:
            api_url = "https://dale.shadowfax.in/api/v3/clients/orders/cancel/"
            print(api_url)

            headers = {
                "Authorization": "Token 7186514310bef086c7e48f8abd482537acc4b553",
                "Content-Type": "application/json",
            }

            body = {
                "request_id": awb_number,
                "cancel_remarks": "Request cancelled by customer",
            }
            print(body)

            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    api_url, headers=headers, json=body, verify=False
                )

            # Parse JSON response
            try:
                response_data = response.json()
                print(response_data)
            except ValueError as e:
                print(str(e))
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # Check API status
            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data.get("message", "Unknown error"),
                )

            if response.status_code == 200:
                print(response)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Order cancelled successfully",
                )

        except DatabaseError as e:
            print(str(e))
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error cancelling shipment: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in tracking",
            )

    @staticmethod
    async def cancel_reverse_shipment(order: Order_Model, awb_number: str):
        try:
            api_url = "https://dale.shadowfax.in/api/v3/clients/orders/cancel/"
            print(api_url)

            headers = {
                "Authorization": "Token 7186514310bef086c7e48f8abd482537acc4b553",
                "Content-Type": "application/json",
            }
            print(awb_number, "<awb_number>")
            body = {
                "request_id": awb_number,
                "cancel_remarks": "Request cancelled by customer",
            }
            async with httpx.AsyncClient(timeout=50, verify=False) as client:
                response = await client.post(api_url, headers=headers, json=body)

            # Parse JSON
            try:
                response_data = response.json()
                print(response_data, "||<response_data>||")
            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # Check API status
            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data.get("message", "Unknown error"),
                )

            # Success
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Order cancelled successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error cancelling shipment: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in tracking",
            )

    @staticmethod
    def tracking_webhook(track_req):

        try:

            from modules.shipment.shipment_service import ShipmentService

            print(track_req)

            db = get_db_session()

            awb_number = track_req.get("awb_number", None)

            if awb_number == None or awb_number == "" or not awb_number:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=False,
                    message="Invalid AWB",
                )

            order = db.query(Order).filter(Order.awb_number == awb_number).first()

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=False,
                    message="Invalid AWB",
                )

            courier_status = track_req.get("event")

            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["sub_status"]
            order.courier_status = courier_status

            new_tracking_info = {
                "status": status_mapping.get(courier_status, {}).get(
                    "sub_status", courier_status
                ),
                "description": track_req.get("comments", ""),
                "subinfo": track_req.get("status", ""),
                "datetime": parse_datetime(
                    Shadowfax.date_formatter(track_req.get("event_timestamp"))
                ).strftime("%d-%m-%Y %H:%M:%S"),
                "location": track_req.get("current_location", ""),
            }

            if not order.tracking_info:
                order.tracking_info = []

            order.tracking_info = [new_tracking_info] + order.tracking_info

            ShipmentService.post_tracking(order)

            order.tracking_response = track_req

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tracking Successfull",
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
                message="Error in tracking",
            )

        finally:
            if db:
                db.close()

    @staticmethod
    def ndr_action(order: Order_Model, awb_number: str):

        try:

            api_url = "https://dale.shadowfax.in/api/v1/clients/order_update/"

            print(api_url)

            headers = {
                "Authorization": "Token " + "7186514310bef086c7e48f8abd482537acc4b553",
                "Content-Type": "application/json",
            }

            body = {
                "awb_number": awb_number,
                "status_update": {"status": "reopen_ndr"},
            }

            response = requests.post(
                api_url, headers=headers, verify=False, json=body, timeout=10
            )

            print(response)

            # If tracking failed at Shiperfecto, return message
            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                print(str(e))
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["message"],
                )

            if response.status_code == 200:

                print(response)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="order cancelled successfully",
                )

        except DatabaseError as e:
            # Log database error

            print(str(e))

            logger.error(
                extra=context_user_data.get(),
                msg="Error creating shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
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
                message="Error in tracking",
            )
