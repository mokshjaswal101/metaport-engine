import http
from psycopg2 import DatabaseError
import pytz
from typing import Dict
import json
import requests
from datetime import datetime
import unicodedata
from pydantic import BaseModel

# import xmltodict
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re

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


def convert_ist_to_utc(ist_datetime_str):
    # Define IST timezone
    ist_tz = pytz.timezone("Asia/Kolkata")

    # Parse the datetime string into a datetime object
    ist_datetime = datetime.strptime(ist_datetime_str, "%d %b, %Y, %H:%M")

    # Localize the datetime to IST
    localized_ist_datetime = ist_tz.localize(ist_datetime)

    # Convert to UTC
    utc_datetime = localized_ist_datetime.astimezone(pytz.utc)

    # Return the UTC datetime as string
    return utc_datetime.strftime("%d-%m-%Y %H:%M:%S")


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


def parse_shipment_tracking(xml_data):
    # Parse the XML data
    root = ET.fromstring(xml_data)

    # Prepare a list to store parsed shipment details
    shipments = []

    # Iterate over each 'Shipment' in the XML
    for shipment in root.findall(".//Shipment"):
        shipment_data = {
            "awb_number": shipment.findtext("AWBNumber", default=""),
            "status": shipment.findtext("Status", default=""),
            "last_update_date": shipment.findtext("LastUpdateDate", default=""),
            "last_update_time": shipment.findtext("LastUpdateTime", default=""),
            "origin": shipment.findtext("Origin", default=""),
            "destination": shipment.findtext("Destination", default=""),
            "recipient": shipment.findtext("Recipient", default=""),
        }
        shipments.append(shipment_data)

    return shipments


def parse_scan_stages(xml_data):
    # Parse the XML data
    root = ET.fromstring(xml_data)

    # List to store scan stages
    scan_stages = []

    # Find all <object model="scan_stages"> elements
    scan_objects = root.findall(".//object[@model='scan_stages']")

    # Iterate over each scan stage object
    for scan in scan_objects:
        scan_stage = {}
        # Extract required fields from each <field> element
        for field in scan.findall("field"):
            name = field.get("name")
            value = field.text
            scan_stage[name] = value

        # Add scan stage to the list
        scan_stages.append(scan_stage)

    # Now convert to the desired output format
    output = []
    for stage in scan_stages:
        output.append(
            {
                "updated_on": stage["updated_on"],
                "status": stage["status"],
                "reason_code": stage["reason_code"],
                "reason_code_number": stage["reason_code_number"],
                "scan_status": stage["scan_status"],
                "location": stage["location"],
                "location_city": stage["location_city"],
                "location_type": stage["location_type"],
                "city_name": stage["city_name"],
                "Employee": stage["Employee"],
            }
        )

    return output


class Ecom:

    @staticmethod
    def Add_Contract_Generate_Token(credentials: Dict[str, str]):
        try:
            print("<<*>>", credentials.get("username"), "<<*>>")
            url = "https://api.ecomexpress.in/apiv2/fetch_awb/"
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            print("0")
            payload = {
                "username": credentials.get("username"),
                "password": credentials.get("password"),
                "count": 1,  # Request 1 AWB for test
                "type": "PPD",
            }
            print("1")
            response = requests.post(url, headers=headers, json=payload)
            print("2")
            print(response)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if "awb_number" in data:
                        return {
                            "status": True,
                            "message": "Credentials are valid",
                            "awb": data,
                        }
                    else:
                        return {
                            "status": False,
                            "message": "Credentials might be valid, but AWB not returned",
                            "response": data,
                        }
                except Exception as e:
                    return {
                        "status": False,
                        "message": "Invalid JSON response",
                        "error": str(e),
                    }

            elif response.status_code == 401:
                return {
                    "status": False,
                    "message": "Invalid credentials (Unauthorized)",
                }

            else:
                return {
                    "status": False,
                    "message": f"Unexpected response {response.status_code}",
                    "response": response.text,
                }

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="There was some issue in Generating AWB, please try again: {}".format(
                    str(e)
                ),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="There was some issue in Generating AWB, please try again",
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
                message="There was some issue in Generating AWB, please try again",
            )

    @staticmethod
    def generate_awb(credentials: Dict[str, str], order: Order_Model):

        try:

            api_url = "https://api.ecomexpress.in/apiv2/fetch_awb/"

            data = {
                "username": credentials["username"],
                "password": credentials["password"],
                "count": 1,
                "type": "PPD" if order.payment_mode.lower() == "prepaid" else "COD",
            }

            try:
                response = requests.post(api_url, data=data)
                # print("lalalalalalalala", response.text)
                response.raise_for_status()  # Raise an exception for HTTP errors
                response = response.json()

            except requests.exceptions.RequestException as e:
                print(f"Error while fetching AWB: {e}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in Generating AWB, please try again",
                )

            # if location creation failed
            if response["success"] != "yes":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in Generating AWB, please try again",
                )

            # if successfully created location

            if response["success"] == "yes":

                awb = response["awb"][0]

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=awb,
                    message="Awb generated",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="There was some issue in Generating AWB, please try again: {}".format(
                    str(e)
                ),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="There was some issue in Generating AWB, please try again",
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
                message="There was some issue in Generating AWB, please try again",
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
            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )

            db = get_db_session()

            combined_address = (
                order.consignee_address.strip() + " " + order.consignee_landmark.strip()
            ).strip()
            # Split into two parts
            consignee_address1 = clean_text(combined_address[:90])
            consignee_landmark = clean_text(combined_address[90:])

            awb = Ecom.generate_awb(credentials, order)

            if awb.status == False:
                return awb

            else:
                awb = awb.data

            body = {
                "AWB_NUMBER": awb,
                "ORDER_NUMBER": "LM/" + str(client_id) + "/" + order.order_id,
                "PRODUCT": (
                    "PPD" if order.payment_mode.lower() == "prepaid" else "COD"
                ),
                "CONSIGNEE": order.consignee_full_name,
                "CONSIGNEE_ADDRESS1": consignee_address1,
                "CONSIGNEE_ADDRESS2": consignee_landmark,
                "CONSIGNEE_ADDRESS3": "",
                "DESTINATION_CITY": order.consignee_city,
                "PINCODE": order.consignee_pincode,
                "STATE": order.consignee_state,
                "MOBILE": order.consignee_phone,
                "TELEPHONE": "",
                "ITEM_DESCRIPTION": ", ".join(
                    product["name"] for product in order.products
                ),
                "PIECES": sum(product["quantity"] for product in order.products),
                "COLLECTABLE_VALUE": (
                    0
                    if order.payment_mode.lower() == "prepaid"
                    else float(order.total_amount)
                ),
                "DECLARED_VALUE": float(order.total_amount),
                "ACTUAL_WEIGHT": float(order.weight),
                "VOLUMETRIC_WEIGHT": float(order.volumetric_weight),
                "LENGTH": float(order.length),
                "BREADTH": float(order.weight),
                "HEIGHT": float(order.height),
                "PICKUP_NAME": str(client_id) + " " + pickup_location.location_name,
                "PICKUP_ADDRESS_LINE1": clean_text(pickup_location.address),
                "PICKUP_ADDRESS_LINE2": clean_text(pickup_location.landmark),
                "PICKUP_PINCODE": pickup_location.pincode,
                "PICKUP_PHONE": pickup_location.contact_person_phone,
                "PICKUP_MOBILE": pickup_location.contact_person_phone,
                "RETURN_NAME": str(client_id) + " " + pickup_location.location_name,
                "RETURN_ADDRESS_LINE1": clean_text(pickup_location.address),
                "RETURN_ADDRESS_LINE2": clean_text(pickup_location.landmark),
                "RETURN_PINCODE": pickup_location.pincode,
                "RETURN_PHONE": pickup_location.contact_person_phone,
                "RETURN_MOBILE": pickup_location.contact_person_phone,
                "DG_SHIPMENT": "false",
                "ADDITIONAL_INFORMATION": {
                    "GST_TAX_CGSTN": "",
                    "GST_TAX_IGSTN": "",
                    "GST_TAX_SGSTN": "",
                    "SELLER_GSTIN": "",
                    "INVOICE_DATE": "12-08-2022",
                    "INVOICE_NUMBER": "",
                    "GST_TAX_RATE_SGSTN": "",
                    "GST_TAX_RATE_IGSTN": "",
                    "GST_TAX_RATE_CGSTN": "",
                    "GST_HSN": "",
                    "GST_TAX_BASE": "",
                    "GST_ERN": "123456789876",
                    "ESUGAM_NUMBER": "",
                    "ITEM_CATEGORY": "Clothes",
                    "GST_TAX_NAME": "",
                    "ESSENTIALPRODUCT": "Y",
                    "PICKUP_TYPE": "WH",
                    "OTP_REQUIRED_FOR_DELIVERY": "Y",
                    "RETURN_TYPE": "WH",
                    "GST_TAX_TOTAL": "",
                    "SELLER_TIN": "",
                    "CONSIGNEE_ADDRESS_TYPE": "GENERAL",
                    "CONSIGNEE_LONG": "",
                    "CONSIGNEE_LAT": "",
                },
            }

            # print(body)

            encoded_data = urlencode(body)

            headers = {
                "username": credentials["username"],
                "password": credentials["password"],
                "json_input": json.dumps([body]),
            }

            # print(headers)

            api_url = "https://api.ecomexpress.in/apiv2/manifest_awb/"

            response = requests.post(api_url, data=headers, verify=False, timeout=10)

            # print(response.text)

            try:
                response_data = response.json()
                print(response_data)

                response = response_data["shipments"][0]
                print(response)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response["success"] != True:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=(
                        response["reason"]
                        if "reason" in response
                        else "Failed to create shipment"
                    ),
                )

            # if order created successfully at shiperfecto
            if response["success"] == True:

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = response["awb"]
                order.aggregator = "ecom-express"
                order.shipping_partner_order_id = response["order_number"]
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - Ecom express",
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
                        "awb_number": response["awb"],
                        "delivery_partner": "ecom-express",
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

            api_url = (
                "https://plapi.ecomexpress.in/track_me/api/mawbd/?username=4PLSCMTECHNOLOGIESPRIVATELIMITED-EXS750154&password=fYdlXkimY6&awb="
                + awb_number
            )

            headers = {"x-webhook-version": "2.0"}

            response = requests.get(api_url, headers=headers, verify=False, timeout=10)

            # If tracking failed at Shiperfecto, return message
            try:
                print(response.text)
                scan_stages = parse_scan_stages(response.text)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = scan_stages[0].get("reason_code_number", "")
            updated_awb_number = order.awb_number

            activites = tracking_data = scan_stages

            db = get_db_session()

            # print(courier_status)

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
                        "status": status_mapping.get(
                            activity.get("reason_code_number", "").strip(), {}
                        ).get("sub_status", activity.get("status").strip()),
                        "description": activity.get("status", ""),
                        "subinfo": activity.get("status", ""),
                        "datetime": convert_ist_to_utc(activity.get("updated_on", "")),
                        "location": activity.get("city_name", ""),
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
                "username": "4PLSCMTECHNOLOGIESPRIVATELIMITED-EXS750154",
                "password": "fYdlXkimY6",
                "awbs": awb_number,
            }

            api_url = "https://api.ecomexpress.in/apiv2/cancel_awb/"

            # body = {"awb": awb_number}

            response = requests.post(api_url, data=headers, verify=False, timeout=10)

            # If tracking failed at Shiperfecto, return message
            try:
                print(response)
                response_data = response.json()
                print(response_data)
                response_data = response_data[0]

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Some error occurred while cancelling, please try again",
                )

            # If tracking failed, return message
            if response_data["success"] != True:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message=(
                        response_data["reason"]
                        if "reason" in response_data
                        else "Failed to cancel shipment"
                    ),
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

            from modules.shipment.shipment_service import ShipmentService

            # print(track_req)

            print(1)

            awb_number = track_req.get("awb", None)

            if awb_number == None or awb_number == "" or not awb_number:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=False,
                    message="Invalid AWB",
                )

            awb_number = str(awb_number)

            ShipmentService.webhook_track_shipment(awb_number=awb_number)

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
