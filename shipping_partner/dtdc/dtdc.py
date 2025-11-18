import http
from psycopg2 import DatabaseError
from typing import Dict, Optional, List
import requests
from datetime import datetime
from fastapi import Request
from sqlalchemy.orm import joinedload
from datetime import datetime, timedelta, timezone
import json
from decimal import Decimal
import pytz
from context_manager.context import context_user_data, get_db_session
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from sqlalchemy import or_

from logger import logger

from modules.ndr.ndr_service import NdrService

# models
from models import (
    Pickup_Location,
    Order,
    COD_Remittance,
    Wallet,
    Wallet_Logs,
    Company_To_Client_Contract,
    Aggregator_Courier,
    Pincode_Serviceability,
)

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# from shipping_partner.dtdc.dtdc_schema import (
#     Dtdc_single_model,
#     Dtdc_mps_model,
#     Dtdc_cancel_model,
#     Dtdc_track_model,
# )

from pydantic import BaseModel


class TempModel(BaseModel):
    client_id: int


# data
from .status_mapping import status_mapping

# from .delivery_partner_mapping import courier_mapping


# service
from modules.wallet.wallet_service import WalletService

from utils.datetime import parse_datetime

# cerdentials=> {"customer_code":"GL017","api_key":"b01ed3562b088ab9c52822e3c18f9e","service_type_id":"B2C PRIORITY","access_token":"OO2567_trk_json:47e341a16fbd29bedfd12495c9385e53"}


def get_next_mwf(date):
    today = datetime.today().date()
    client_id = context_user_data.get().client_id

    days = 5
    if client_id == 26:
        days = 3
    elif client_id == 71:
        days = 4

    # Add D+X (days after the given date based on client_id)
    d_plus_x = date + timedelta(days=days)

    # Ensure the date is not in the past
    if d_plus_x < today:
        d_plus_x = today

    # Check if D+X is already Monday (0), Wednesday (2), or Friday (4)
    if d_plus_x.weekday() in {0, 2, 4}:
        return d_plus_x

    # Find the next available Monday, Wednesday, or Friday
    for i in range(1, 7):
        candidate_date = d_plus_x + timedelta(days=i)
        if candidate_date.weekday() in {0, 2, 4}:
            return candidate_date


def update_or_create_cod_remittance(order, db):
    delivered_status = "delivered"

    delivered_date = None
    for status_info in order.tracking_info:
        if status_info["status"] == delivered_status:
            datetime_str = status_info["datetime"]

            # Try to parse the datetime with both formats
            for date_format in ["%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                try:
                    delivered_date = datetime.strptime(datetime_str, date_format).date()
                    break  # If successful, exit the loop
                except ValueError:
                    continue  # Try the next format if ValueError occurs

            # If the datetime string doesn't match either format
            if delivered_date is None:
                raise ValueError(f"Unrecognized datetime format: {datetime_str}")

    # Calculate the next Wednesday after D+5
    remittance_date = get_next_mwf(delivered_date)

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


class Dtdc:

    Dtdc_username = "GL9132_trk_json"
    Dtdc_password = "mBmYo"
    Dtdc_token_base_url = "https://blktracksvc.dtdc.com/"  # end url with slash /

    # GenerateToken
    def Add_Contract_Generate_Token(credentials: Dict[str, str]):
        try:
            print(
                "we are in dtdc generate token",
                credentials["username"],
                credentials["password"],
            )
            # API URL
            url = (
                Dtdc.Dtdc_token_base_url
                + "dtdc-api/api/dtdc/authenticate?username="
                + credentials["username"]
                + "&password="
                + credentials["password"]
            )
            # print(url, "<url>")
            response = requests.request("GET", url)
            #  Debug prints
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            try:
                print(f"Response JSON: {response.json()}")
            except Exception:
                print("Response is not in JSON format.")

            print(response.status_code, "<status_code>")
            #  Check response
            if response.status_code == 200:
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
            logger.error(
                "Error: Unable to connect to the DTDC API. Check the URL or network connection."
            )
            return {"error": "Unable to connect to the DTDC API"}

        except Timeout:
            logger.error("Error: The request to the DTDC API timed out.")
            return {"error": "Request timed out"}

        except HTTPError as http_err:
            logger.error("HTTP error occurred", http_err)
            return {"error": f"HTTP error: {http_err}"}

        except RequestException as req_err:
            logger.error("An error occurred", req_err)
            return {"error": f"Request error: {req_err}"}

        except Exception as e:
            logger.error("Unexpected error", e)
            return {"error": "An unexpected error occurred"}

    # GenerateToken
    def generate_token():
        try:
            # API URL
            url = (
                Dtdc.Dtdc_token_base_url
                + "dtdc-api/api/dtdc/authenticate?username="
                + Dtdc.Dtdc_username
                + "&password="
                + Dtdc.Dtdc_password
            )
            response = requests.request("GET", url)
            response.raise_for_status()
            return response.text

        except ConnectionError:
            logger.error(
                "Error: Unable to connect to the DTDC API. Check the URL or network connection."
            )
            return {"error": "Unable to connect to the DTDC API"}

        except Timeout:
            logger.error("Error: The request to the DTDC API timed out.")
            return {"error": "Request timed out"}

        except HTTPError as http_err:
            logger.error("HTTP error occurred", http_err)
            return {"error": f"HTTP error: {http_err}"}

        except RequestException as req_err:
            logger.error("An error occurred", req_err)
            return {"error": f"Request error: {req_err}"}

        except Exception as e:
            logger.error("Unexpected error", e)
            return {"error": "An unexpected error occurred"}

    def date_formatter(input_date, input_time):
        try:
            # Parse the input date
            parsed_date = datetime.strptime(input_date, "%d%m%Y")

            # Determine time format based on length
            if len(input_time) == 4:
                parsed_time = datetime.strptime(input_time, "%H%M")
                hour, minute, second = parsed_time.hour, parsed_time.minute, 0
            elif len(input_time) == 6:
                parsed_time = datetime.strptime(input_time, "%H%M%S")
                hour, minute, second = (
                    parsed_time.hour,
                    parsed_time.minute,
                    parsed_time.second,
                )
            else:
                return f"Unsupported time format: {input_time}"

            # Combine date and time
            combined_datetime = datetime(
                year=parsed_date.year,
                month=parsed_date.month,
                day=parsed_date.day,
                hour=hour,
                minute=minute,
                second=second,
            )

            # Return formatted datetime
            return combined_datetime.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            return f"Error in formatting: {e}"

    @staticmethod
    def create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):

        client_id = context_user_data.get().client_id

        try:
            # get the location code for shiperfecto from the db
            db = get_db_session()
            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )
            print(1)

            pickup_serviceability = (
                db.query(Pincode_Serviceability)
                .filter(
                    Pincode_Serviceability.pincode == pickup_location.pincode,
                )
                .first()
            )

            if pickup_serviceability is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Pickup location not serviceable",
                )

            if delivery_partner.slug == "dtdc-air":
                pickup_serviceability = pickup_serviceability.dtdc_air_fm
            else:
                pickup_serviceability = pickup_serviceability.dtdc_surface_fm

            if pickup_serviceability == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Pickup location not serviceable",
                )

            delivery_serviceability = (
                db.query(Pincode_Serviceability)
                .filter(
                    Pincode_Serviceability.pincode == order.consignee_pincode,
                )
                .first()
            )

            if delivery_serviceability is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Destination pincode not serviceable",
                )

            if delivery_partner.slug == "dtdc-air":

                if order.payment_mode.lower() == "prepaid":
                    delivery_serviceability = (
                        delivery_serviceability.dtdc_air_lm_prepaid
                    )
                else:
                    delivery_serviceability = delivery_serviceability.dtdc_air_lm_cod

            else:
                if order.payment_mode.lower() == "prepaid":
                    delivery_serviceability = (
                        delivery_serviceability.dtdc_surface_lm_prepaid
                    )
                else:
                    delivery_serviceability = (
                        delivery_serviceability.dtdc_surface_lm_cod
                    )

            if delivery_serviceability == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Destination pincode not serviceable",
                )

            delivery_partner.slug

            body = json.dumps(
                {
                    "consignments": [
                        {
                            "customer_code": credentials["customer_code"],
                            "service_type_id": (
                                "B2C PRIORITY"
                                if delivery_partner.slug == "dtdc-air"
                                else "B2C SMART EXPRESS"
                            ),
                            "load_type": "NON-DOCUMENT",
                            "description": "",
                            "dimension_unit": "cm",
                            "length": float(order.length),
                            "width": float(order.breadth),
                            "height": float(order.height),
                            "weight_unit": "kg",
                            "weight": (
                                3.0
                                if (
                                    delivery_partner.slug == "dtdc 3kg"
                                    and float(order.weight) < 3.0
                                )
                                else float(order.weight)
                            ),
                            "declared_value": float(order.total_amount),
                            "num_pieces": len(order.products),
                            "origin_details": {
                                "name": pickup_location.location_name,
                                "phone": pickup_location.contact_person_phone,
                                "alternate_phone": (
                                    pickup_location.alternate_phone
                                    if pickup_location.alternate_phone
                                    else pickup_location.contact_person_phone
                                ),
                                "address_line_1": pickup_location.address,
                                "address_line_2": pickup_location.landmark,
                                "pincode": pickup_location.pincode,
                                "city": pickup_location.city,
                                "state": pickup_location.state,
                            },
                            "destination_details": {
                                "name": order.consignee_full_name,
                                "phone": order.consignee_phone,
                                "alternate_phone": (
                                    order.consignee_alternate_phone
                                    if order.consignee_alternate_phone
                                    else order.consignee_phone
                                ),
                                "address_line_1": order.consignee_address,
                                "address_line_2": order.consignee_landmark,
                                "pincode": order.consignee_pincode,  # order.consignee_pincode
                                "city": order.consignee_city,
                                "state": order.consignee_state,
                            },
                            "customer_reference_number": "LMI/"
                            + str(client_id)
                            + "/"
                            + order.order_id
                            + (
                                f"/{str(order.cancel_count)}"
                                if order.cancel_count > 0
                                else ""
                            ),
                            "cod_collection_mode": (
                                ""
                                if order.payment_mode.lower() == "prepaid"
                                else "CASH"
                            ),
                            "cod_amount": (
                                0
                                if order.payment_mode.lower() == "prepaid"
                                else float(order.total_amount)
                            ),
                            "commodity_id": "",
                            "reference_number": "",
                        }
                    ]
                }
            )

            print("body", body)

            print(2)

            # print(body)

            api_url = "https://dtdcapi.shipsy.io/api/customer/integration/consignment/softdata"
            headers = {
                "api-key": credentials["api_key"],
                "Content-Type": "application/json",
            }
            response = requests.request(
                "POST", api_url, headers=headers, data=body, timeout=10
            )

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

            print("dtdc response", response_data)

            # If order creation failed at DTDC, return message
            if (
                response_data.get("status", None) is None
                or response_data["status"] != "OK"
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Could not create the shipment. Please try again later",
                )

            data = response_data.get("data", [{}])[0]

            if data.get("success", None) == None or data["success"] == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=data.get(
                        "message", "Could not create shipment, please try again later"
                    ),
                )

            # if order created successfully at DTDC

            print(4)

            if data.get("success", None) == True:
                # print(response_data, "||data||")

                print(5)
                # update status
                order.status = "booked"
                order.sub_status = "booked"
                order.courier_status = "BOOKED"

                order.awb_number = data["reference_number"]
                order.aggregator = "dtdc"
                order.shipping_partner_order_id = ""
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + delivery_partner.slug,
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
                        "awb_number": data["reference_number"],
                        "delivery_partner": "dtdc",
                    },
                    message="AWB assigned successfully",
                )

            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg="DTDC Error is status is not OK: {}".format(str(response_data)),
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["data"],
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="DTDC Error posting shipment: {}".format(str(e)),
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
                msg="DTDC Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def dev_create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):

        client_id = context_user_data.get().client_id

        try:
            # get the location code for shiperfecto from the db
            db = get_db_session()
            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )
            print(1)
            delivery_partner.slug
            body = json.dumps(
                {
                    "consignments": [
                        {
                            "customer_code": "GL017",
                            "service_type_id": "B2C PRIORITY",
                            "load_type": "NON-DOCUMENT",
                            "description": "test",
                            "dimension_unit": "cm",
                            "length": "70.0",
                            "width": "70.0",
                            "height": "65.0",
                            "weight_unit": "kg",
                            "weight": "17.0",
                            "declared_value": "5982.6",
                            "num_pieces": "1",
                            "origin_details": {
                                "name": "TESTENTERPRISES",
                                "phone": "9465637062",
                                "alternate_phone": "9465637062",
                                "address_line_1": "dummysender",
                                "address_line_2": "",
                                "pincode": "110046",
                                "city": "NewDelhi",
                                "state": "Delhi",
                            },
                            "destination_details": {
                                "name": "TEST",
                                "phone": "9465637062",
                                "alternate_phone": "9465637062",
                                "address_line_1": "testreceiver",
                                "address_line_2": "",
                                "pincode": "636010",
                                "city": "SALEM",
                                "state": "TamilNadu",
                            },
                            "return_details": {
                                "address_line_1": "Test_Address_Return",
                                "address_line_2": "Test_Address_Returnline2",
                                "city_name": "DELHI",
                                "name": "Test_Return",
                                "phone": "9465637062",
                                "pincode": "248001",
                                "state_name": "DELHI",
                                "email": "amisha.arora@test.co.in",
                                "alternate_phone": "9465637062",
                            },
                            "customer_reference_number": "order_id",
                            "cod_collection_mode": "",
                            "cod_amount": "",
                            "commodity_id": "99",
                            "eway_bill": "12345678",
                            "is_risk_surcharge_applicable": "false",
                            "invoice_number": "AB001",
                            "invoice_date": "14Oct2022",
                            "reference_number": "",
                        }
                    ]
                }
            )
            # print(body)

            api_url = "https://demodashboardapi.shipsy.in/api/customer/integration/consignment/softdata"
            headers = {
                "api-key": "b01ed3562b088ab9c52822e3c18f9e",
                "Content-Type": "application/json",
            }
            response = requests.request(
                "POST", api_url, headers=headers, data=body, timeout=10
            )

            try:
                response_data = response.json()
                # print(response_data)
            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # print("dtdc response", response_data)

            # If order creation failed at DTDC, return message
            if (
                response_data.get("status", None) is None
                or response_data["status"] != "OK"
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Could not create the shipment. Please try again later",
                )

            data = response_data.get("data", [{}])[0]

            if data.get("success", None) == None or data["success"] == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=data.get(
                        "message", "Could not create shipment, please try again later"
                    ),
                )

            # if order created successfully at DTDC

            print(4)

            if data.get("success", None) == True:
                # print(response_data, "||data||")

                print(5)
                # update status
                order.status = "booked"
                order.sub_status = "booked"
                order.courier_status = "BOOKED"

                order.awb_number = data["reference_number"]
                order.aggregator = "dtdc"
                order.shipping_partner_order_id = ""
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + delivery_partner.slug,
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
                        "awb_number": data["reference_number"],
                        "delivery_partner": "dtdc",
                    },
                    message="AWB assigned successfully",
                )

            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg="DTDC Error is status is not OK: {}".format(str(response_data)),
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["data"],
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="DTDC Error posting shipment: {}".format(str(e)),
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
                msg="DTDC Unhandled error: {}".format(str(e)),
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
            print(0)
            headers = {
                "api-key": "a051202eb85d1331e19284f2ceb1cb",  # credentials["api_key"]
                "Content-Type": "application/json",
            }
            api_url = (
                "https://dtdcapi.shipsy.io/api/customer/integration/consignment/cancel"
            )

            print(1)
            body = json.dumps(
                {"AWBNo": [awb_number], "customerCode": "GL9132"}
            )  # credentials["customer_code"]

            response = requests.request("POST", api_url, headers=headers, data=body)

            print(2)

            try:
                response_data = response.json()
                print(response_data)
                print(3)
            except ValueError as e:
                logger.error(
                    "DTDC cancel_shipment Failed to parse JSON response: %s", e
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Some error occurred while cancelling, please try again",
                )
            # If tracking failed, return message
            if response_data["success"] == False:
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

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str):
        try:
            token = Dtdc.generate_token()

            print("token", token)

            if "error" in token:
                logger.error("DTDC generate_token token error: %s", token["error"])
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=token["error"],
                )
            else:
                logger.info("DTDC track_shipment token: %s", token)

                api_url = (
                    Dtdc.Dtdc_token_base_url + "dtdc-api/rest/JSONCnTrk/getTrackDetails"
                )

                headers = {
                    "X-Access-Token": token,
                    "Content-Type": "application/json",
                }
                payload = json.dumps(
                    {"trkType": "cnno", "strcnno": awb_number, "addtnlDtl": "Y"}
                )  # X09384961

                logger.info("DTDC track_shipment payload ready to post: %s", payload)

                response = requests.request(
                    "POST", api_url, headers=headers, data=payload
                )

                try:
                    response_data = response.json()
                    print(1)
                except ValueError as e:
                    logger.error("DTDC Failed to parse JSON response: %s", e)
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Some error occurred while tracking, please try again",
                    )

                # print("2", response_data)

                # If tracking failed, return message
                if response_data["status"] == "FAILED":
                    print("3")
                    logger.error(
                        "DTDC track_shipment if response is not true: %s", response_data
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=response_data,
                    )

                tracking_data = response_data.get("trackDetails", "")
                tracking_header = response_data.get("trackHeader", "")
                logger.info(
                    "DTDC track_shipment api response: %s",
                    response_data,
                )
                print(4)

                # if tracking_data is not present in the respnse
                if not tracking_data:
                    print(5)
                    logger.error(
                        "DTDC track_shipment if tracking_data is not true: %s",
                        tracking_data,
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Some error occurred while tracking, please try again",
                    )

                print(6)

                courier_status = tracking_data[len(tracking_data) - 1].get(
                    "strCode", ""
                )

                updated_awb_number = tracking_header.get("strShipmentNo", "")
                activites = tracking_data

                print(7)

                db = get_db_session()

                old_status = order.status
                old_sub_status = order.sub_status
                # update the order status, and awb if different

                if courier_status in status_mapping:
                    order.status = status_mapping[courier_status].get("status", None)
                    order.sub_status = status_mapping[courier_status].get(
                        "sub_status", None
                    )
                else:
                    order.status = old_status
                    order.sub_status = old_sub_status

                order.courier_status = courier_status

                order.awb_number = (
                    updated_awb_number if updated_awb_number else order.awb_number
                )

                ist = pytz.timezone("Asia/Kolkata")
                utc = pytz.utc

                edd = tracking_header["strExpectedDeliveryDate"]
                local_time = (
                    ist.localize(datetime.strptime(edd, "%d%m%Y")) if edd else None
                )
                order.edd = local_time.astimezone(utc) if local_time else None

                # update the tracking info
                if activites:
                    new_tracking_info = [
                        {
                            "status": (
                                status_mapping[activity.get("strCode", "")].get(
                                    "sub_status", ""
                                )
                                if activity.get("strCode") in status_mapping
                                else ""
                            ),
                            "description": activity.get("strAction"),
                            "datetime": parse_datetime(
                                Dtdc.date_formatter(
                                    activity["strActionDate"],
                                    activity["strActionTime"],
                                )
                            ).strftime("%d-%m-%Y %H:%M:%S"),
                            "location": activity.get("strOrigin", ""),
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
    def tracking_webhook(response_data):

        from modules.shipment.shipment_service import ShipmentService

        try:

            tracking_data = response_data.get("shipmentStatus", "")
            tracking_header = response_data.get("shipment", "")

            courier_status = tracking_data[len(tracking_data) - 1].get("strAction", "")

            updated_awb_number = tracking_header.get("strShipmentNo", "")
            activites = tracking_data

            db = get_db_session()

            order = (
                db.query(Order).filter(Order.awb_number == updated_awb_number).first()
            )

            if order is None:
                logger.error(
                    extra=context_user_data.get(),
                    msg="DTDC track_shipment order not found: %s",
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Order not found",
                )

            old_status = order.status
            old_sub_status = order.sub_status

            if courier_status in status_mapping:
                order.status = status_mapping[courier_status].get("status", None)
                order.sub_status = status_mapping[courier_status].get(
                    "sub_status", None
                )
            else:
                order.status = old_status
                order.sub_status = old_sub_status

            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            ist = pytz.timezone("Asia/Kolkata")
            utc = pytz.utc

            edd = tracking_header["strExpectedDeliveryDate"]
            local_time = ist.localize(datetime.strptime(edd, "%d%m%Y")) if edd else None
            order.edd = local_time.astimezone(utc) if local_time else None

            # update the tracking info
            if activites:
                new_tracking_info = [
                    {
                        "status": (
                            status_mapping[activity.get("strAction", "")].get(
                                "sub_status", ""
                            )
                            if activity.get("strAction") in status_mapping
                            else ""
                        ),
                        "description": activity.get("strActionDesc"),
                        "datetime": parse_datetime(
                            Dtdc.date_formatter(
                                activity["strActionDate"],
                                activity["strActionTime"],
                            )
                        ).strftime("%d-%m-%Y %H:%M:%S"),
                        "location": activity.get("strOrigin", ""),
                    }
                    for activity in activites
                ]

                new_tracking_info_com = new_tracking_info + (order.tracking_info or [])
                order.tracking_info = new_tracking_info_com

            tracking_response = GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": updated_awb_number,
                    "current_status": order.status,
                },
                message="Tracking successfull",
            )

            ShipmentService.post_tracking(order)

            order.tracking_response = response_data

            db.add(order)
            db.commit()

            return tracking_response

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
