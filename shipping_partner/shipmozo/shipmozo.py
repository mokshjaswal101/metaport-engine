import http
from psycopg2 import DatabaseError
from typing import Dict
from pydantic import BaseModel
import requests
from datetime import datetime
import base64
import pytz
import unicodedata
import re

from context_manager.context import context_user_data, get_db_session

from logger import logger

# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# data
from .status_mapping import status_mapping
from .delivery_partner_mapping import courier_mapping

# service
from modules.wallet.wallet_service import WalletService


from utils.datetime import parse_datetime


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


class Shipmozo:

    @staticmethod
    def create_pickup_location(pickup_location_id: int, credentials: Dict[str, str]):

        print("inside the terror")

        try:

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == pickup_location_id)
                .first()
            )

            api_url = "https://shipping-api.com/app/api/v1/create-warehouse"

            client_id = context_user_data.get().client_id

            body = {
                "address_title": str(client_id) + " " + pickup_location.location_name,
                "name": pickup_location.contact_person_name,
                "phone": pickup_location.contact_person_phone,
                "alternate_phone": "",
                "email": pickup_location.contact_person_email,
                "address_line_one": clean_text(pickup_location.address),
                "address_line_two": clean_text(pickup_location.landmark),
                "pin_code": pickup_location.pincode,
            }

            print(body)

            headers = {
                "public-key": credentials["public_key"],
                "private-key": credentials["private_key"],
            }

            response = requests.post(api_url, json=body, headers=headers, verify=False)

            response_data = response.json()

            print(response_data)

            # if location creation failed
            if response_data["result"] == "0":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

            # if successfully created location
            if response_data["result"] == "1":
                shipmozo_location_id = response_data["data"]["warehouse_id"]

                pickup_location.courier_location_codes = {
                    **pickup_location.courier_location_codes,
                    "shipmozo": shipmozo_location_id,
                }

                db.add(pickup_location)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=shipmozo_location_id,
                    message="Location created successfully",
                )

            # in case of any other issue
            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating location at shiperfecto: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Error occurred while creating location",
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
    def create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):

        try:

            client_id = context_user_data.get().client_id

            # get the location code for shiperfecto from the db

            db = get_db_session()

            courier_location_codes = (
                db.query(Pickup_Location.courier_location_codes)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )[0]

            # get the logistify location code from the db
            shipmozo_pickup_location = courier_location_codes.get("shipmozo", None)

            # if no shipmozo location code mapping is found for the current pickup location, create a new warehouse at logistify
            if shipmozo_pickup_location is None:

                shipmozo_pickup_location = Shipmozo.create_pickup_location(
                    order.pickup_location_code, credentials
                )

                # if could not create location at shiperfecto, throw error
                if shipmozo_pickup_location.status == False:

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Unable to place order. Please try again later",
                    )

                else:
                    shipmozo_pickup_location = shipmozo_pickup_location.data

            shipmozo_order_id = order.shipping_partner_order_id

            if shipmozo_order_id is None:

                consignee_address1 = clean_text(order.consignee_address.strip())
                consignee_landmark = clean_text(order.consignee_landmark.strip())

                body = {
                    "order_id": "LMO/"
                    + str(client_id)
                    + "/"
                    + order.order_id
                    + (f"/{str(order.cancel_count)}" if order.cancel_count > 0 else ""),
                    "order_date": str(order.order_date),
                    "order_type": "ESSENTIALS",
                    "consignee_name": order.consignee_full_name,
                    "consignee_phone": order.consignee_phone,
                    "consignee_alternate_phone": order.consignee_alternate_phone,
                    "consignee_email": (
                        order.consignee_email
                        if order.consignee_email
                        else "xyz@gmail.com"
                    ),
                    "consignee_address_line_one": consignee_address1,
                    "consignee_address_line_two": consignee_landmark,
                    "consignee_pin_code": order.consignee_pincode,
                    "consignee_city": order.consignee_city,
                    "consignee_state": order.consignee_state,
                    "product_detail": [
                        {
                            "name": product["name"][:200],
                            "sku_number": product["sku_code"],
                            "quantity": product["quantity"],
                            "unit_price": product["unit_price"],
                            "discount": "",
                            "hsn": "",
                            "product_category": "",
                        }
                        for product in order.products
                    ],
                    "payment_type": (
                        "PREPAID" if order.payment_mode.lower() == "prepaid" else "COD"
                    ),
                    "cod_amount": (
                        0
                        if order.payment_mode.lower() == "prepaid"
                        else float(order.total_amount)
                    ),
                    "weight": float(order.weight * 1000),
                    "length": float(order.length),
                    "width": float(order.breadth),
                    "height": float(order.height),
                    "warehouse_id": shipmozo_pickup_location,
                    "gst_ewaybill_number": "",
                    "gstin_number": "",
                }

                headers = {
                    "public-key": credentials["public_key"],
                    "private-key": credentials["private_key"],
                    "Content-Type": "application/json",
                }

                print(body)

                api_url = "https://shipping-api.com/app/api/v1/push-order"

                response = requests.post(
                    api_url, json=body, headers=headers, verify=True, timeout=60
                )

                print(response.text)

                try:
                    response_data = response.json()
                    print(response_data)

                except ValueError as e:
                    logger.error("Failed to parse JSON response: %s", e)
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Some error occurred while assigning AWB, please try again",
                    )

                # If order creation failed at Logistify, return message
                if response_data["result"] == "0":
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=response_data["data"]["error"],
                    )

                shipmozo_order_id = response_data["data"]["order_id"]

                order.shipping_partner_order_id = shipmozo_order_id
                db.add(order)
                db.commit()

            # assign AWB

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )

            print(pickup_location.pincode)
            assign_courier_body = {
                "order_id": shipmozo_order_id,
                "courier_id": (
                    "98"
                    if (
                        (
                            pickup_location.pincode == "560021"
                            or pickup_location.pincode == "700012"
                        )
                        and delivery_partner.aggregator_slug == "301"
                    )
                    else delivery_partner.aggregator_slug
                ),
            }

            print(assign_courier_body)

            headers = {
                "public-key": credentials["public_key"],
                "private-key": credentials["private_key"],
                "Content-Type": "application/json",
            }

            api_url = "https://shipping-api.com/app/api/v1/assign-courier"

            response = requests.post(
                api_url,
                json=assign_courier_body,
                headers=headers,
                verify=True,
                timeout=60,
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            if response_data["result"] == "1":

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = response_data["data"]["awb_number"]
                order.aggregator = "shipmozo"
                order.shipping_partner_order_id = shipmozo_order_id
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + response_data["data"]["courier"],
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
                        "awb_number": response_data["data"]["awb_number"],
                        "delivery_partner": response_data["data"]["courier"],
                    },
                    message="AWB assigned successfully",
                )

            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["message"],
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
    def track_shipment(order: Order_Model, awb_number: str):

        try:

            print(awb_number)

            headers = {
                "private-key": "LvIj3ASUDJlxkTeqo8Kt",
                "public-key": "Rtg8OZXA1DQYk9mvfljM",
            }

            api_url = f"""https://shipping-api.com/app/api/v1/track-order?awb_number={awb_number}"""

            print(api_url)

            response = requests.get(api_url, headers=headers, verify=True, timeout=60)

            print(response.text)

            # If tracking failed at Logistify, return message
            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            if response_data["result"] == 0:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message=response_data["message"],
                )

            tracking_data = response_data.get("data", None)

            # if tracking_data is not present in the respnse
            if not tracking_data:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = tracking_data.get("current_status", "")
            updated_awb_number = tracking_data.get("awb_number", "")

            activites = tracking_data.get("scan_detail", "")

            db = get_db_session()

            old_status = order.status
            old_sub_status = order.sub_status

            # update the order status, and awb if different
            order.status = (
                status_mapping[courier_status].get("status", None) or old_status
            )
            order.sub_status = (
                status_mapping[courier_status].get("sub_status", None) or old_sub_status
            )
            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            ist = pytz.timezone("Asia/Kolkata")
            utc = pytz.utc

            edd = tracking_data["expected_delivery_date"]
            if edd:
                try:
                    local_time = ist.localize(datetime.strptime(edd, "%Y-%m-%d"))
                    order.edd = local_time.astimezone(utc)
                except ValueError:
                    order.edd = None  # Handle invalid date format
            else:
                order.edd = None  # Handle missing key

            # update the tracking info
            if activites:
                new_tracking_info = [
                    {
                        "status": status_mapping.get(
                            activity.get("status", "").strip(), {}
                        ).get("status", activity.get("status").strip()),
                        "description": "",
                        "subinfo": activity.get("status", ""),
                        "datetime": parse_datetime(activity.get("date")).strftime(
                            "%d-%m-%Y %H:%M:%S"
                        ),
                        "location": activity.get("location", ""),
                    }
                    for activity in activites
                ]

                # new_tracking_info.reverse()

                print(new_tracking_info)

                order.tracking_info = new_tracking_info

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": updated_awb_number,
                    "current_status": order.status,
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
    def cancel_shipment(order: Order_Model, awb_number: str):

        try:

            db = get_db_session()

            api_url = f"""https://shipping-api.com/app/api/v1/cancel-order"""

            body = {
                "order_id": order.shipping_partner_order_id,
                "awb_number": awb_number,
            }

            headers = {
                "public-key": "Rtg8OZXA1DQYk9mvfljM",
                "private-key": "LvIj3ASUDJlxkTeqo8Kt",
            }

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=60
            )

            # If tracking failed at Logistify, return message
            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # if location creation failed
            if response_data["result"] == "0":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Could not cancel shipment",
                )

            # if successfully created location
            if response_data["result"] == "1":

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="order cancelled successfully",
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
    def tracking_webhook(track_req):

        try:

            from modules.shipment.shipment_service import ShipmentService

            print(track_req)

            db = get_db_session()

            awb_number = track_req.get("awb_number", None)

            if awb_number == None or awb_number == "" or not awb_number:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Invalid AWB",
                )

            order = db.query(Order).filter(Order.awb_number == awb_number).first()

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Invalid AWB",
                )

            courier_status = track_req.get("current_status")

            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["sub_status"]
            order.courier_status = courier_status

            new_tracking_info = {
                "status": status_mapping.get(courier_status, {}).get(
                    "status", courier_status
                ),
                "description": track_req.get("status", ""),
                "subinfo": track_req.get("status", ""),
                "datetime": parse_datetime(track_req.get("status_time")).strftime(
                    "%d-%m-%Y %H:%M:%S"
                ),
                "location": track_req["status_feed"]["scan"][0]["location"],
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
