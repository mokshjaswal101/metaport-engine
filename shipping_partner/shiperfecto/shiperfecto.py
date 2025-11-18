import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
import pytz
from datetime import datetime
import unicodedata
from pydantic import BaseModel

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re
import json
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# data
from .status_mapping import status_mapping

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


class Shiperfecto:

    def Add_Contract_Generate_Token(credentials):
        try:
            headers = {
                "Authorization": "Token " + credentials.get("token", ""),
                "Content-Type": "application/json",
            }

            api_url = "http://app.shiperfecto.com/api/v1/cancel-order"
            body = {"awb": "SPX1234567890"}  # sample awb for testing

            response = requests.post(
                api_url, headers=headers, json=body, verify=False, timeout=10
            )

            #  Debug prints
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            try:
                print(f"Response JSON: {response.json()}")
            except Exception:
                print("Response is not in JSON format.")

            print(response.status_code, "<status_code>")
            #  Check response
            if response.status_code == 400:
                return {
                    "status_code": 200,
                    "status": True,
                    "message": "Token is valid",
                    # "data": response.json(),
                }

            elif response.status_code in [401, 403]:
                return {
                    "status_code": response.status_code,
                    "status": False,
                    "message": "Invalid or expired token",
                    "data": response.text,
                }

            else:
                #  Return actual code and response for debugging
                return {
                    "status_code": response.status_code,
                    "status": False,
                    "message": "Unexpected response from API",
                    "data": response.text,
                }

        except ConnectionError:
            print(" Connection error occurred (network issue)")
            return {
                "status_code": http.HTTPStatus.BAD_REQUEST,
                "status": False,
                "message": "Unable to connect to Shiperfecto API.",
            }

        except Timeout:
            print(" Request timed out")
            return {
                "status_code": http.HTTPStatus.REQUEST_TIMEOUT,
                "status": False,
                "message": "Request timed out.",
            }

        except HTTPError as http_err:
            print(f" HTTP error occurred: {http_err}")
            return {
                "status_code": http.HTTPStatus.BAD_REQUEST,
                "status": False,
                "message": f"HTTP error: {http_err}",
            }

        except RequestException as req_err:
            print(f" Request exception: {req_err}")
            return {
                "status_code": http.HTTPStatus.BAD_REQUEST,
                "status": False,
                "message": f"Request error: {req_err}",
            }

        except Exception as e:
            print(f" Unexpected error: {e}")
            return {
                "status_code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "status": False,
                "message": f"Unexpected error: {e}",
            }

    @staticmethod
    def create_pickup_location(pickup_location_id: int, credentials: Dict[str, str]):

        try:

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == pickup_location_id)
                .first()
            )

            api_url = "http://app.shiperfecto.com/api/v1/create-warehouse"

            client_id = context_user_data.get().client_id

            body = {
                "business_name": str(client_id) + " " + pickup_location.location_name,
                "sender_name": pickup_location.contact_person_name,
                "phone": pickup_location.contact_person_phone,
                "pincode": pickup_location.pincode,
                "email": pickup_location.contact_person_email,
                "address": pickup_location.address,
                "city": pickup_location.city,
                "state": pickup_location.state,
            }

            headers = {
                "Authorization": "Token " + credentials["api_key"],
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, json=body, headers=headers, verify=False)

            response_data = response.json()

            # if location creation failed
            if response_data["status"] == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

            # if successfully created location

            if response_data["status"] == True:
                shiperfecto_location_id = response_data["warehouse_id"]

                pickup_location.courier_location_codes = {
                    **pickup_location.courier_location_codes,
                    "shiperfecto": shiperfecto_location_id,
                }

                db.add(pickup_location)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=shiperfecto_location_id,
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

            print("pickup location", str(e))
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

            print("inside shiperfecto")

            # get the location code for shiperfecto from the db

            db = get_db_session()

            courier_location_codes = (
                db.query(Pickup_Location.courier_location_codes)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )[0]

            # get the shiperfecto location code from the db
            shiperfecto_pickup_location = courier_location_codes.get(
                "shiperfecto", None
            )

            print(shiperfecto_pickup_location)

            # if no shiperfecto location code mapping is found for the current pickup location, create a new warehouse at shiperfecto
            if shiperfecto_pickup_location is None:

                shiperfecto_pickup_location = Shiperfecto.create_pickup_location(
                    order.pickup_location_code, credentials
                )

                # if could not create location at shiperfecto, throw error
                if shiperfecto_pickup_location.status == False:

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Unable to place order. Please try again later",
                    )

                else:
                    shiperfecto_pickup_location = shiperfecto_pickup_location.data

                    # Initialize variables with default values
            consignee_address1 = ""
            consignee_landmark = ""

            consignee_address1 = clean_text(order.consignee_address.strip())
            consignee_landmark = clean_text(
                order.consignee_landmark.strip() if order.consignee_landmark else ""
            )

            print("yoyoy")

            body = {
                "order_id": "LM/"
                + str(client_id)
                + "/"
                + str(order.id)
                + (f"/{str(order.cancel_count)}" if order.cancel_count > 0 else ""),
                "consignee_full_name": order.consignee_full_name,
                "consignee_primary_contact": order.consignee_phone,
                "consignee_email": (
                    order.consignee_email if order.consignee_email else "abc@gmail.com"
                ),
                "order_type": "FORWARD",
                "consignee_address1": consignee_address1,
                "consignee_address2": consignee_landmark,
                "consignee_address_type": "warehouse",
                "pincode": order.consignee_pincode,
                "city": order.consignee_city,
                "state": order.consignee_state,
                "invoice_number": "inv/"
                + str(client_id)
                + "/"
                + str(order.id)
                + (f"/{str(order.cancel_count)}" if order.cancel_count > 0 else ""),
                "payment_mode": (
                    "prepaid" if order.payment_mode.lower() == "prepaid" else "cod"
                ),
                "express_type": delivery_partner.mode,
                "products": [
                    {
                        "product_name": product["name"],
                        "product_qty": str(product["quantity"]),
                        "product_val": str(product["unit_price"]),
                        "product_sku": product["sku_code"],
                    }
                    for product in order.products
                ],
                "order_amount": str(order.order_value),
                "total_amount": str(order.total_amount),
                "cod_amount": (
                    "0"
                    if order.payment_mode.lower() == "prepaid"
                    else str(order.total_amount)
                ),
                "tax_amount": str(order.tax_amount if order.tax_amount else 0),
                "order_weight": str(order.applicable_weight),
                "order_length": str(order.length),
                "order_width": str(order.breadth),
                "order_height": str(order.height),
                "pickup_address_id": shiperfecto_pickup_location,
                "return_address_id": shiperfecto_pickup_location,
                "delivery_partner": delivery_partner.aggregator_slug,
            }

            print(body)

            headers = {
                "Authorization": "Token " + credentials["api_key"],
                "Content-Type": "application/json",
            }

            api_url = "https://app.shiperfecto.com/api/v1/create-order"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

            print("dfasdfasdf", response.text)

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response_data["status"] == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=(
                        "Failed to create shipment"
                        if response_data["order_data"]["error"] == "Server Error"
                        else response_data["order_data"]["error"]
                    ),
                )

            # if order created successfully at shiperfecto
            if response_data["status"] == True:

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = response_data["order_data"]["awb"]
                order.aggregator = "shiperfecto"
                order.shipping_partner_order_id = response_data["order_data"][
                    "order_id"
                ]
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - "
                    + response_data["order_data"]["delivery_partner"],
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
                        "awb_number": response_data["order_data"]["awb"],
                        "delivery_partner": response_data["order_data"][
                            "delivery_partner"
                        ],
                    },
                    message="AWB assigned successfully",
                )

            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["order_data"]["error"],
                )

        except DatabaseError as e:
            # Log database error

            print(str(e))
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

        # return GenericResponseModel(
        #     status_code=http.HTTPStatus.OK,
        #     status=True,
        #     data={
        #         "awb_number": awb_number,
        #         "current_status": "delivered",
        #     },
        #     message="Tracking successfull",
        # )

        try:
            headers = {
                "Authorization": "Token " + "Nd5yU3yienFHsoQ3jnYSYJdLCUefPI2awS7hZIOK",
                "Content-Type": "application/json",
            }

            api_url = "http://app.shiperfecto.com/api/v1/tracking?awb=" + awb_number

            response = requests.get(api_url, headers=headers, verify=False, timeout=10)

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
            if response_data["success"] == False:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["tracking"]["status"],
                )

            tracking_data = response_data.get("tracking", "")

            # if tracking_data is not present in the respnse
            if not tracking_data:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = tracking_data.get("status", "")
            updated_awb_number = tracking_data.get("awb", "")

            activites = tracking_data.get("events", "")

            db = get_db_session()

            # print(courier_status)

            # update the order status, and awb if different
            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["sub_status"]
            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            ist = pytz.timezone("Asia/Kolkata")
            utc = pytz.utc

            edd = tracking_data.get("estimated_delivery_date", None)

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
                        "description": activity.get("description", ""),
                        "subinfo": activity.get("status", ""),
                        "datetime": parse_datetime(
                            activity.get("status_datetime", "")
                        ).strftime("%d-%m-%Y %H:%M:%S"),
                        "location": activity.get("status_location", ""),
                    }
                    for activity in activites
                ]

                # new_tracking_info.reverse()

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
    def cancel_shipment(order: Order_Model, awb_number: str):

        try:
            headers = {
                "Authorization": "Token " + "Nd5yU3yienFHsoQ3jnYSYJdLCUefPI2awS7hZIOK",
                "Content-Type": "application/json",
            }

            api_url = "http://app.shiperfecto.com/api/v1/cancel-order"

            body = {"awb": awb_number}

            response = requests.post(
                api_url, headers=headers, json=body, verify=False, timeout=10
            )

            # If tracking failed at Shiperfecto, return message
            try:
                print(response)
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Some error occurred while cancelling, please try again",
                )

            # If tracking failed, return message
            if response_data["status"] == False:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Failed to cancel shipment",
                )

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

            print(track_req)

            db = get_db_session()

            print(1)

            awb_number = track_req.get("awb_no", None)

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

            client_id = order.client_id

            context_user_data.set(TempModel(**{"client_id": client_id}))

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
                message="An internal server error occurred. Please try again later.",
            )
