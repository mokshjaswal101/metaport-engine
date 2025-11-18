import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
from datetime import datetime
import base64
import unicodedata
from pydantic import BaseModel
import pytz
import json
from utils.datetime import parse_datetime


from concurrent.futures import ThreadPoolExecutor
import http

executor = ThreadPoolExecutor()

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re

# models
from models import Pickup_Location, Order, Courier_Routing_Code
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

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


class Shiprocket:

    def Add_Contract_Generate_Token(credentials):
        try:
            api_url = "https://apiv2.shiprocket.in/v1/external/auth/login"

            body = {"email": credentials["email"], "password": credentials["password"]}

            headers = {
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, json=body, headers=headers, verify=False)
            data = response.json()  # convert response to dict
            print(response.text, "<<response>>")
            #  Debug prints
            if "errors" in data:
                return {
                    "status_code": http.HTTPStatus.BAD_REQUEST,
                    "status": False,
                    "message": f"Error: {data['message']}",
                }
            else:
                return {
                    "status_code": 200,
                    "status": True,
                    "message": "Token is valid",
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
                "business_name": str(client_id)
                + "/"
                + clean_text(pickup_location.location_name),
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
    def get_token(credentials: Dict[str, str]):

        try:

            db = get_db_session()

            api_url = "https://apiv2.shiprocket.in/v1/external/auth/login"

            body = {"email": credentials["email"], "password": credentials["password"]}

            headers = {
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, json=body, headers=headers, verify=False)

            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message=response.json()["message"],
                )

            response_data = response.json()

            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=True,
                message="success",
                data=response_data.get("token"),
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

            # if client_id == 93:
            #     return GenericResponseModel(
            #         status=False, status_code=400, message="Non serviceable"
            #     )

            # get the location code for shiperfecto from the db

            token = Shiprocket.get_token(credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            db = get_db_session()

            print("token", token)

            courier_location_codes = (
                db.query(Pickup_Location.courier_location_codes)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )[0]

            print(order.pickup_location_code)

            # get the shiperfecto location code from the db
            # shiprocket_location_code = courier_location_codes.get("shiprocket", None)

            # if no shiperfecto location code mapping is found for the current pickup location, create a new warehouse at shiperfecto

            pickup_location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == order.pickup_location_code,
                )
                .first()
            )

            # return

            courier_id = ""
            if delivery_partner.aggregator_slug == "amazon":
                courier_id = 142 if order.payment_mode.lower() == "prepaid" else 195

            else:
                courier_id = delivery_partner.aggregator_slug

            combined_pickup_adderess = (
                order.consignee_address.strip()
                + " "
                + (order.consignee_landmark.strip() if order.consignee_landmark else "")
            )

            shipping_address1 = clean_text(combined_pickup_adderess[:60])
            shipping_landmark = clean_text(combined_pickup_adderess[60:])

            body = {
                "order_id": "LM/"
                + str(client_id)
                + "/"
                + order.order_id
                + (f"/{str(order.cancel_count)}" if order.cancel_count > 0 else ""),
                "order_date": order.order_date.strftime("%d-%m-%Y %H:%M:%S"),
                "billing_customer_name": order.consignee_full_name,
                "billing_last_name": "",
                "billing_address": shipping_address1,
                "billing_address_2": shipping_landmark,
                "billing_city": order.consignee_city,
                "billing_pincode": order.consignee_pincode,
                "billing_state": order.consignee_state,
                "billing_country": "India",
                "billing_email": (
                    order.consignee_email if order.consignee_email else "xyz@gmail.com"
                ),
                "billing_phone": order.consignee_phone,
                "shipping_is_billing": True,
                "order_items": [
                    {
                        "name": product["name"][:50],
                        "sku": product["sku_code"][:45] + str(index)
                        or (product["name"][:45] + str(index)),
                        "units": product["quantity"],
                        "selling_price": product["unit_price"],
                    }
                    for index, product in enumerate(order.products)
                ],
                "payment_method": (
                    "Prepaid" if order.payment_mode.lower() == "prepaid" else "COD"
                ),
                "sub_total": float(order.total_amount),
                "length": (
                    10
                    if delivery_partner.aggregator_slug == "amazon"
                    else float(order.length)
                ),
                "breadth": (
                    10
                    if delivery_partner.aggregator_slug == "amazon"
                    else float(order.breadth)
                ),
                "height": (
                    10
                    if delivery_partner.aggregator_slug == "amazon"
                    else float(order.height)
                ),
                "weight": (
                    0.49
                    if delivery_partner.aggregator_slug == "amazon"
                    else float(order.weight)
                ),
                "pickup_location": str(client_id)
                + " "
                + clean_text(pickup_location.location_name),
                "vendor_details": {
                    "email": pickup_location.contact_person_email,
                    "phone": pickup_location.contact_person_phone,
                    "name": clean_text(pickup_location.contact_person_name).replace(
                        "-", " "
                    ),
                    "address": pickup_location.address,
                    "address_2": pickup_location.landmark,
                    "city": pickup_location.city,
                    "state": pickup_location.state,
                    "country": "India",
                    "pin_code": pickup_location.pincode,
                    "pickup_location": str(client_id)
                    + " "
                    + clean_text(pickup_location.location_name),
                },
                "courier_id": courier_id,
            }
            print(body)

            # return

            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
            }

            api_url = "https://apiv2.shiprocket.in/v1/external/shipments/create/forward-shipment"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=60
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

            # If order creation failed at Shiperfecto, return message
            if response_data["status"] != 1:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Failed to create shipment",
                )

            # if order created successfully at shiperfecto
            if response_data["status"] == 1:

                response_data = response_data["payload"]

                if response_data.get("awb_assign_error", None) is not None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Failed to create shipment",
                    )

                if response_data.get("awb_code", None) is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Failed to create shipment",
                    )

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = response_data["awb_code"]
                order.aggregator = "shiprocket"
                order.shipping_partner_order_id = response_data["order_id"]
                order.shipping_partner_shipping_id = response_data["shipment_id"]
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

                if delivery_partner and "bluedart" in delivery_partner.slug.lower():
                    codes = (
                        db.query(Courier_Routing_Code)
                        .filter(
                            Courier_Routing_Code.pincode == order.consignee_pincode,
                        )
                        .first()
                    )

                    routing_code = codes.bluedart_routing_code if codes else ""
                    cluster_code = codes.bluedart_cluster_code if codes else ""

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["awb_code"],
                        "delivery_partner": delivery_partner.slug,
                        "routing_code": routing_code if routing_code else "",
                        "cluster_code": cluster_code if cluster_code else "",
                    },
                    message="AWB assigned successfully",
                )

            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Failed to create shipment",
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
    def track_shipment(order: Order_Model, awb_number: str, credentials=None):

        try:

            if not credentials:

                if (
                    order.courier_partner != "bluedart 2kg"
                    and order.courier_partner != "bluedart 1.5kg"
                    and order.courier_partner != "delhivery 2kg"
                ):

                    credentials = {
                        "email": "lastmiles@warehousity.com",
                        "password": "L@stMiles@981",
                    }

                else:
                    credentials = {
                        "email": "lastmiles2@warehousity.com",
                        "password": "L@stMiles@981",
                    }

            token = Shiprocket.get_token(credentials)

            print(token)

            if token.status == False:
                return token

            else:
                token = token.data

            api_url = (
                "https://apiv2.shiprocket.in/v1/external/courier/track/awb/"
                + awb_number
            )

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }

            response = requests.get(api_url, headers=headers, verify=False, timeout=60)

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

            response_data = response_data.get("tracking_data", None)

            # If tracking failed, return message
            if response_data["track_status"] != 1:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Could not track AWB",
                )

            courier_status = response_data.get("shipment_status", "")
            updated_awb_number = response_data["shipment_track"][0]["awb_code"]

            activites = response_data.get("shipment_track_activities", "")

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

            edd = response_data["shipment_track"][0]["edd"]
            local_time = ist.localize(datetime.strptime(edd, "%Y-%m-%d %H:%M:%S"))
            order.edd = local_time.astimezone(utc)

            # update the tracking info
            if activites:

                new_tracking_info = [
                    {
                        "status": status_mapping.get(
                            (
                                int(activity.get("sr-status", "0"))
                                if str(activity.get("sr-status", "0")).isdigit()
                                else None
                            ),
                            {},
                        ).get(
                            "sub_status", activity.get("sr-status-label", "").strip()
                        ),
                        "description": activity.get("status", ""),
                        "subinfo": activity.get("status", ""),
                        "datetime": parse_datetime(activity.get("date")).strftime(
                            "%d-%m-%Y %H:%M:%S"
                        ),
                        "location": activity.get("location", ""),
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

            if (
                order.courier_partner != "bluedart 2kg"
                and order.courier_partner != "delhivery 2kg"
                and order.courier_partner != "delhivery 1.5kg"
                and order.courier_partner != "bluedart 1.5kg"
            ):

                credentials = {
                    "email": "lastmiles@warehousity.com",
                    "password": "L@stMiles@981",
                }

            else:
                credentials = {
                    "email": "lastmiles2@warehousity.com",
                    "password": "L@stMiles@981",
                }

            token = Shiprocket.get_token(credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            # Shiprocket API URL
            url = "https://apiv2.shiprocket.in/v1/external/orders/cancel/shipment/awbs"

            # Headers for the API request
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }

            # Payload for the API request
            payload = {"awbs": [order.awb_number]}

            response = requests.post(
                url, headers=headers, json=payload, verify=False, timeout=60
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

    def generate_shipping_label(order: Order_Model):

        print("insinsininin")
        """
        Generates a shipping label for the given order.

        Args:
            order (dict): The order details, expected to have `shipping_partner_shipping_id`.
            token (str): The Bearer token for authentication.

        Returns:
            dict: The response from the Shiprocket API.


        """

        credentials = {
            "email": "lastmiles@warehousity.com",
            "password": "L@stMiles@981",
        }

        token = Shiprocket.get_token(credentials)

        if token.status == False:
            return token

        else:
            token = token.data

        print("token", token)

        db = get_db_session()

        # Extract the shipping ID from the order
        shipment_id = order.shipping_partner_shipping_id
        if not shipment_id:
            raise ValueError(
                "Order must have a 'shipping_partner_shipping_id' key with a valid value."
            )

        # Shiprocket API URL
        url = "https://apiv2.shiprocket.in/v1/external/courier/generate/label"

        # Headers for the API request
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        # Payload for the API request
        payload = {"shipment_id": [shipment_id]}

        try:
            # Make the POST request
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

            print(response.json())

            label_64 = pdf_to_base64(response.json()["label_url"])

            return label_64  # Return the JSON response
        except requests.exceptions.RequestException as e:
            # Handle exceptions (e.g., network issues, bad responses)
            raise RuntimeError(f"Failed to generate shipping label: {e}")

    @staticmethod
    def tracking_webhook(track_req):

        try:

            from modules.shipment.shipment_service import ShipmentService

            db = get_db_session()

            awb_number = track_req.get("awb", None)

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

            courier_status = track_req.get("shipment_status_id")

            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["sub_status"]
            order.courier_status = courier_status

            new_tracking_info = {
                "status": status_mapping.get(courier_status, {}).get(
                    "sub_status", track_req.get("shipment_status", "")
                ),
                "description": track_req.get("shipment_status", ""),
                "subinfo": track_req.get("shipment_status", ""),
                "datetime": parse_datetime(track_req.get("date")).strftime(
                    "%d-%m-%Y %H:%M:%S"
                ),
                "location": track_req["scans"][-1]["location"],
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


import requests


def pdf_to_base64(s3_url):
    """
    Downloads a PDF from the given URL and converts it to Base64 format.

    Args:
        s3_url (str): The URL of the PDF file.

    Returns:
        str: The Base64 encoded string of the PDF content.
    """
    try:
        # Fetch the PDF from the URL
        response = requests.get(s3_url)
        response.raise_for_status()  # Raise an error for HTTP issues

        # Get the binary content of the PDF
        pdf_binary = response.content

        # Encode the binary content to Base64
        base64_pdf = base64.b64encode(pdf_binary).decode("utf-8")

        return base64_pdf
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch the PDF: {e}")
