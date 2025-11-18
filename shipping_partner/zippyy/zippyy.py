import http
from psycopg2 import DatabaseError
from typing import Dict, Optional, List
import requests
from datetime import datetime
from fastapi import Request
import json
import unicodedata
import pytz
from context_manager.context import context_user_data, get_db_session
import re
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from logger import logger

# models
from marketplace.easyecom.easyecom_schema import credentials
from models import Pickup_Location, Order, Pincode_Serviceability

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# data
from .status_mapping import status_mapping

from utils.datetime import parse_datetime

# utils
from utils.datetime import convert_ist_to_utc


def clean_text(text):
    if text is None:
        return ""
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
    # Replace all special characters except comma and hyphen with a space
    text = re.sub(r"[^a-zA-Z0-9\s,-]", " ", text)
    # Replace multiple spaces with a single space
    return re.sub(r"\s+", " ", text).strip()


def convert_timestamp_to_datetime(ms_timestamp: int) -> str:
    # Convert milliseconds to seconds
    seconds = ms_timestamp / 1000

    # Convert to datetime object
    dt = datetime.fromtimestamp(seconds)

    # Format as dd-mm-yyyy hh:mm:ss
    return dt.strftime("%d-%m-%Y %H:%M:%S")


class Zippyy:

    # API URL'S
    staging_base_url = "https://sandbox.sellingpartnerapi-in.zippyy.ai"
    production_base_url = "https://sellingpartnerapi-in.zippyy.ai"

    def date_formatter(timestamp):
        try:
            # Try parsing with fractional seconds
            dt_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # Fallback to parsing without fractional seconds
            dt_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

        # Format to the desired output
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_token(credentials: Dict[str, str]):

        try:

            db = get_db_session()

            api_url = Zippyy.production_base_url + "/v1/external/auth/login"

            body = {
                "emailAddress": credentials["email"],
                "password": credentials["password"],
            }

            headers = {"Content-Type": "application/json", "x-api-version": "1"}

            response = requests.post(api_url, json=body, headers=headers, verify=False)

            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message=response.json()["message"],
                )

            response_data = response.json()

            print(response_data)

            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=True,
                message="success",
                data=response_data.get("accessToken"),
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

    def Add_Contract_Generate_Token(credentials):
        try:
            url = api_url = Zippyy.production_base_url + "/v1/external/auth/login"

            body = {
                "emailAddress": credentials["email"],
                "password": credentials["password"],
            }

            headers = {"Content-Type": "application/json", "x-api-version": "1"}

            response = requests.post(api_url, json=body, headers=headers, verify=False)
            data = response.json()
            print(data, "||<<response>>||")
            if "message" in data:
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

    # orderCreate delhivery
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

            token = Zippyy.get_token(credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            db = get_db_session()

            print(order.pickup_location_code)

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

            created_at = int(order.order_date.timestamp())

            body = {
                "orderNumber": "L-"
                + str(client_id)
                + "-"
                + str(order.id)
                + (f"-{str(order.cancel_count)}" if order.cancel_count > 0 else ""),
                "orderCreatedAt": created_at,
                "channelId": "External",
                "sender": {
                    "firstName": pickup_location.contact_person_name,
                    "lastName": "",
                    "email": pickup_location.contact_person_email,
                    "phoneNumber": pickup_location.contact_person_phone,
                    "companyName": pickup_location.location_name,
                },
                "origin": {
                    "addressLine1": clean_text(pickup_location.address),
                    "addressLine2": clean_text(
                        pickup_location.landmark or pickup_location.city
                    ),
                    "city": pickup_location.city,
                    "state": pickup_location.state,
                    "country": "India",
                    "countryCode": "IN",
                    "pinCode": pickup_location.pincode,
                    "type": "Work",
                },
                "receiver": {
                    "firstName": order.consignee_full_name,
                    "lastName": "",
                    "email": order.consignee_email,
                    "phoneNumber": order.consignee_phone,
                    "companyName": order.consignee_company,
                },
                "destination": {
                    "addressLine1": clean_text(order.consignee_address),
                    "addressLine2": clean_text(
                        order.consignee_landmark or order.consignee_city
                    ),
                    "city": order.consignee_city,
                    "state": order.consignee_state,
                    "country": "India",
                    "countryCode": "IN",
                    "pinCode": order.consignee_pincode,
                    "type": "Work",
                },
                "returnAddress": {
                    "addressLine1": clean_text(pickup_location.address),
                    "addressLine2": clean_text(
                        pickup_location.landmark or pickup_location.city
                    ),
                    "city": pickup_location.city,
                    "state": pickup_location.state,
                    "country": "India",
                    "countryCode": "IN",
                    "pinCode": pickup_location.pincode,
                    "type": "Work",
                },
                "type": "External",
                "sellerNote": "",
                "parcelAttributes": {
                    "dimension": {
                        "length": float(order.length),
                        "width": float(order.breadth),
                        "height": float(order.height),
                        "unit": "cm",
                    },
                    "weight": {
                        "weight": (
                            float(order.weight) if float(order.weight) > 0.25 else 0.25
                        ),
                        "unit": "kg",
                    },
                },
                "tags": "",
                "productRequestsList": [
                    {
                        "productName": product["name"],
                        "price": str(product["unit_price"]),
                        "quantity": product["quantity"],
                        "currencyCode": "INR",
                        "sku": str(
                            product["sku_code"] if product["sku_code"] else None
                        ),
                        "taxRate": "0",
                        "discount": "0",
                        "description": None,
                        "hs_code": None,
                        "category": None,
                    }
                    for product in order.products
                ],
                "shippingProperties": {
                    "orderType": (
                        "PREPAID" if order.payment_mode.lower() == "prepaid" else "COD"
                    ),
                    "subTotal": float(order.total_amount),
                    "shippingCharges": 0,
                    "otherCharges": 0,
                    "discount": 0,
                },
                "carrier": "zippyyxekart",
                "service": "Surface 1.0kg",
            }

            print(body)

            # return

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
            }

            api_url = (
                Zippyy.production_base_url + "/v1/external/shipments/forward-shipment"
            )

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=60
            )

            # print(response.text)

            try:
                response_data = response.json()
                # print(response_data)

                text = response_data.get("message", [""])[0]
                match = re.search(r"message\s*:(.*)", text)
                if match:
                    extracted_message = match.group(1).strip()
                    print(extracted_message)
                    if (
                        "pin code is not serviceable" in extracted_message.lower()
                        or "pincode is not serviceable" in extracted_message.lower()
                    ):
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message="Pincode not serviceable by Ekart.",
                        )

            except ValueError as e:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Failed to parse JSON response from Zippyy: {e}, Response text: {response.text}",
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # Check if AWB key is present in the response - if not, throw error
            if "awb" not in response_data:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"AWB key missing in Zippyy response: {response_data}",
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Failed to create shipment",
                )

            # If order creation failed at Zippyy based on AWB response
            if not response_data.get("awb"):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Failed to create shipment",
                )

            # update status
            order.status = "booked"
            order.sub_status = "shipment booked"
            order.courier_status = "BOOKED"

            order.awb_number = response_data["awb"]
            order.aggregator = "zippyy"
            order.shipping_partner_order_id = response_data["orderId"]
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
                    "awb_number": response_data["awb"],
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
    def create_reverse_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):
        try:
            print(1)
            # logger.info("Delhivery create_order token: %s", Delhivery.Token)

            client_id = context_user_data.get().client_id

            # get the location code for delhivery from the db

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )
            # get the delhivery location code from the db

            delhivery_pickup_location = pickup_location.courier_location_codes.get(
                "delhivery",
                None,
            )

            # if no delhivery location code mapping is found for the current pickup location, create a new warehouse at delhivery
            if delhivery_pickup_location is None:

                delhivery_pickup_location = Delhivery.create_pickup_location(
                    order.pickup_location_code, credentials, delivery_partner
                )

                # if could not create location at delhivery, throw error
                if delhivery_pickup_location.status == False:

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Unable to place order. Please try again later",
                    )

                else:
                    delhivery_pickup_location = delhivery_pickup_location.data

            print("location", delhivery_pickup_location)

            # return GenericResponseModel(
            #     status_code=http.HTTPStatus.BAD_REQUEST,
            #     message="test",
            # )

            make_data_string = {
                "shipments": [
                    {
                        "name": order.consignee_full_name,
                        "add": clean_text(order.consignee_address),
                        "pin": order.consignee_pincode,
                        "city": order.consignee_city,
                        "state": order.consignee_state,
                        "country": "India",
                        "phone": order.consignee_phone,
                        "order": "LMO/" + str(client_id) + "/" + order.order_id,
                        "payment_mode": "Pickup",
                        "products_desc": clean_text(order.products[0]["name"]),
                        "hsn_code": "",
                        "cod_amount": (
                            0
                            if order.payment_mode.lower() == "prepaid"
                            else float(order.total_amount)
                        ),
                        "order_date": order.order_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "total_amount": float(order.total_amount),
                        "quantity": sum(
                            product["quantity"] for product in order.products
                        ),
                        "shipment_length": float(order.length),
                        "shipment_width": float(order.breadth),
                        "shipment_height": float(order.height),
                        "weight": float(order.weight * 1000),
                        "shipping_mode": (
                            "express"
                            if delivery_partner == "delhivery-air"
                            else "surface"
                        ),
                        "address_type": "",
                    }
                ],
                "pickup_location": {
                    "name": delhivery_pickup_location,
                    "add": clean_text(pickup_location.address),
                    "city": pickup_location.city,
                    "pin_code": int(pickup_location.pincode),
                    "country": "India",
                    "phone": pickup_location.contact_person_phone,
                },
            }
            # Serialize JSON object to a string
            json_string = json.dumps(make_data_string)

            # Concatenate with the prefix
            body = f"format=json&data={json_string}"
            print(body)
            print(2)
            api_url = Delhivery.create_order_url
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Token " + credentials["token"],
            }
            print("headers", headers)
            response = requests.request("POST", api_url, headers=headers, data=body)
            print(3)
            try:
                response_data = response.json()
                print(response_data)
            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )
            # If order creation failed at Delhivery, return message
            if response_data["packages"][0]["status"] != "Success":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=list(response_data.values()),
                )

            # if order created successfully at Delhivery

            print(4)

            if response_data["packages"][0]["status"] == "Success":
                print(response_data, "||data||")
                db = get_db_session()
                print(5)
                # update status
                order.status = "pickup"
                order.sub_status = "pickup pending"
                order.courier_status = "pickup"

                order.awb_number = response_data["packages"][0]["waybill"]
                order.aggregator = "delhivery"
                order.shipping_partner_order_id = ""
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + str("Delhivery"),
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }

                # update the activity
                print(6)

                order.action_history.append(new_activity)
                db.add(order)
                db.commit()
                print(7)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["packages"][0]["waybill"],
                        "delivery_partner": response_data,
                    },
                    message="AWB assigned successfully",
                )

            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg="Delhivery Error is status is not OK: {}".format(
                        str(response_data)
                    ),
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["data"],
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Delhivery Error posting shipment: {}".format(str(e)),
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
                msg="Delhivery Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def cancel_shipment(
        order: Order_Model,
        awb_number: str,
        # credentials: Dict[str, str],
    ):
        try:

            client_id = context_user_data.get().client_id

            credentials = {"email": "project@warehousity.com", "password": "Ankit@166"}

            token = Zippyy.get_token(credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            db = get_db_session()

            orderId = order.shipping_partner_order_id

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
            }

            api_url = (
                Zippyy.production_base_url
                + "/v1/external/shipments/"
                + str(orderId)
                + "/cancel"
            )

            response = requests.put(api_url, headers=headers, verify=False, timeout=60)
            print(response)

            try:
                response_data = response.json()
                print(response_data)
                print(3)
            except Exception as e:
                print(str(e))
                logger.error(
                    "Zippyy cancel_shipment Failed to parse JSON response: %s", e
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Could not cancel shipment, please try again",
                )

            # Check HTTP response status code instead of response_data["status"]
            if response.status_code != 200:
                print(4)
                logger.error(
                    "Zippyy cancel_shipment API returned error: %s", response_data
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Failed to cancel shipment",
                )

            print(5)
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="order cancelled successfully",
                data=response_data,
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating shipment: {}".format(str(e)),
            )
            print(str(e))
            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
            )

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str):
        try:
            client_id = context_user_data.get().client_id

            # if client_id == 93:
            #     return GenericResponseModel(
            #         status=False, status_code=400, message="Non serviceable"
            #     )

            # get the location code for shiperfecto from the db

            credentials = {"email": "project@warehousity.com", "password": "Ankit@166"}

            token = Zippyy.get_token(credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
            }
            # logger.info("Api Ready to post %s", api_url)

            api_url = (
                "https://sellingpartnerapi-in.zippyy.ai/v1/external/shipments/track?orderId="
                + order.shipping_partner_order_id
            )

            response = requests.request(
                "GET",
                api_url,
                headers=headers,
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Delhivery Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = response_data["status"]["subStatus"]
            tracking_data = response_data.get("checkpointList", "")

            print(tracking_data)

            # if tracking_data is not present in the respnse
            if not tracking_data:
                print(5)
                logger.error(
                    "Zippy track_shipment if tracking_data is not true: %s",
                    tracking_data,
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            db = get_db_session()

            order.courier_status = courier_status
            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["status"]

            # update the tracking info
            if tracking_data:
                new_tracking_info = []
                for activity in tracking_data:
                    try:
                        new_tracking_info.append(
                            {
                                "status": status_mapping[
                                    activity["status"]["subStatus"]
                                ]["sub_status"],
                                "description": activity["message"],
                                "subinfo": "",
                                "datetime": convert_timestamp_to_datetime(
                                    int(activity["checkpointEventTime"])
                                ),
                                "location": activity["address"]["rawLocation"],
                            }
                        )
                    except Exception as e:
                        # Log the error if needed
                        print(f"Skipping activity due to error: {e}")
                        pass

                new_tracking_info.reverse()
                order.tracking_info = new_tracking_info

            print("hi i Have reached here")

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": order.awb_number,
                    "current_status": order.status,
                },
                message="Tracking successful",
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

            payload = track_req.get("Shipment", None)

            awb_number = payload.get("AWB", None)

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

            scan_type = payload["Status"]["StatusType"]
            courier_status = payload.get("NSLCode")

            new_status = status_mapping[scan_type][courier_status].get("status", None)

            if not new_status:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=False,
                    message="Invalid Status",
                )

            order.status = status_mapping[scan_type][courier_status]["status"]
            order.sub_status = status_mapping[scan_type][courier_status]["sub_status"]

            order.courier_status = courier_status

            new_tracking_info = {
                "status": order.sub_status,
                "description": payload["Status"]["Instructions"],
                "subinfo": payload["Status"]["Status"],
                "datetime": parse_datetime(
                    Delhivery.date_formatter(payload["Status"]["StatusDateTime"])
                ).strftime("%d-%m-%Y %H:%M:%S"),
                "location": payload["Status"]["StatusLocation"],
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
    def ndr_action(
        order: Order_Model,
        awb_number: str,
        # credentials: Dict[str, str],
    ):
        try:

            courier = order.courier_partner

            token = ""

            if courier == "delhivery-air":
                token = "d21f3e7f152d6d1a1e9f7655b5d4a1bc38c393e1"

            elif (
                courier == "delhivery 5kg"
                or courier == "delhivery 10kg"
                or courier == "delhivery 15kg"
                or courier == "delhivery 20kg"
            ):
                token = "0e203da7ec3308920a659cb1fbf5d156eb9603b0"

            else:
                token = Delhivery.Token

            print(0)
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "token " + token,
            }
            api_url = Delhivery.cancel_order_url

            print(1)
            body = json.dumps({"waybill": awb_number, "act": "RE-ATTEMPT"})

            logger.info("Delhivery ndr_attempt api payload %s", body)

            response = requests.request("POST", api_url, headers=headers, data=body)
            print(2)

            print(headers)

            try:
                response_data = response.json()
                print(response_data)
                print(3)
            except ValueError as e:
                logger.error(
                    "Delhivery ndr_attempt Failed to parse JSON response: %s", e
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Some error occurred while rettempting, please try again",
                )
            # If tracking failed, return message
            if response_data["status"] == "Failure":
                print(4)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Failed to cancel shipment",
                )
            print(5)
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
