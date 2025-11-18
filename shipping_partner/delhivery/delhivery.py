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

from logger import logger

# models
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


class Delhivery:

    # API URL'S
    create_order_url = "https://track.delhivery.com/api/cmu/create.json"

    track_order_url = "https://track.delhivery.com/api/v1/packages/json/"

    cancel_order_url = "https://track.delhivery.com/api/p/edit"

    ndr_url = "https://track.delhivery.com/api/p/update"

    create_pickup_location_url = (
        "https://track.delhivery.com/api/backend/clientwarehouse/create/"
    )

    Token = "801d1b47ec66a5db7568fb03e628ad160dc84ea8"

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
    def create_pickup_location(
        pickup_location_id: int,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):
        try:
            print(1)
            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == pickup_location_id)
                .first()
            )
            print(2)
            api_url = Delhivery.create_pickup_location_url

            client_id = context_user_data.get().client_id
            print(3)
            body = json.dumps(
                {
                    "name": str(client_id)
                    + " "
                    + clean_text(pickup_location.location_name),
                    "email": pickup_location.contact_person_email,
                    "phone": pickup_location.contact_person_phone,
                    "address": clean_text(pickup_location.address),
                    "city": clean_text(pickup_location.city),
                    "country": "India",
                    "pin": pickup_location.pincode,
                    "return_address": clean_text(pickup_location.address),
                    "return_pin": pickup_location.pincode,
                    "return_city": pickup_location.city,
                    "return_state": pickup_location.state,
                    "return_country": "India",
                }
            )

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "token " + credentials["token"],
            }
            logger.info(
                "Delhivery create_pickup_location payload ready to post %s",
                body,
                headers,
            )
            print(4)
            response = requests.request("POST", api_url, headers=headers, data=body)

            print(response)

            response_data = response.json()

            print(response.json())

            # if location creation failed
            if response_data["success"] == False:
                print(5)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

            # if successfully created location
            print(6)
            if response_data["success"] == True:
                Delhivery_location_name = response_data["data"]["name"]

                if delivery_partner.aggregator_slug == "delhivery-air":

                    pickup_location.courier_location_codes = {
                        **pickup_location.courier_location_codes,
                        "delhivery-air": Delhivery_location_name,
                    }

                else:
                    pickup_location.courier_location_codes = {
                        **pickup_location.courier_location_codes,
                        "delhivery": Delhivery_location_name,
                    }

                db.add(pickup_location)
                db.commit()
                print(7)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=Delhivery_location_name,
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
                msg="Error creating location at Delhivery: {}".format(str(e)),
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

    # orderCreate delhivery
    @staticmethod
    def create_order(
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

            delhivery_pickup_location = (
                pickup_location.courier_location_codes.get(
                    "delhivery-air",
                    None,
                )
                if delivery_partner.aggregator_slug == "delhivery-air"
                else pickup_location.courier_location_codes.get(
                    "delhivery",
                    None,
                )
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

            pickup_serviceability = pickup_serviceability.delhivery_fm

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

            if order.payment_mode.lower() == "prepaid":
                delivery_serviceability = delivery_serviceability.delhivery_lm_prepaid
            else:
                delivery_serviceability = delivery_serviceability.delhivery_lm_cod

            if delivery_serviceability == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Destination pincode not serviceable",
                )

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
                        "order": "LMI/"
                        + str(client_id)
                        + "/"
                        + order.order_id
                        + (
                            f"/{str(order.cancel_count)}"
                            if order.cancel_count > 0
                            else ""
                        ),
                        "payment_mode": (
                            "Pre-paid"
                            if order.payment_mode.lower() == "prepaid"
                            else "COD"
                        ),
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

            api_url = Delhivery.create_order_url
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Token " + credentials["token"],
            }

            response = requests.request("POST", api_url, headers=headers, data=body)

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
                    message=response_data["packages"][0]["remarks"][0],
                )

            if response_data["packages"][0]["status"] == "Success":
                print(response_data, "||data||")
                db = get_db_session()
                print(5)
                # update status
                order.status = "booked"
                order.sub_status = "booked"
                order.courier_status = "BOOKED"

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
                        "delivery_partner": delivery_partner.slug,
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
                    message=response_data["packages"][0]["remarks"][0],
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
            body = json.dumps({"waybill": awb_number, "cancellation": True})

            logger.info("Delhivery cancel_shipment api payload %s", body)

            response = requests.request("POST", api_url, headers=headers, data=body)
            print(2)

            print(headers)

            try:
                response_data = response.json()
                print(response_data)
                print(3)
            except ValueError as e:
                logger.error(
                    "Delhivery cancel_shipment Failed to parse JSON response: %s", e
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Some error occurred while cancelling, please try again",
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

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str):
        try:
            logger.info("Delhivery track_shipment token: %s", Delhivery.Token)

            courier = order.courier_partner

            token = ""

            if courier == "delhivery-air":
                token = "d21f3e7f152d6d1a1e9f7655b5d4a1bc38c393e1"

            if (
                courier == "delhivery 5kg"
                or courier == "delhivery 10kg"
                or courier == "delhivery 15kg"
                or courier == "delhivery 20kg"
            ):
                token = "0e203da7ec3308920a659cb1fbf5d156eb9603b0"

            api_url = Delhivery.track_order_url + "?waybill=" + awb_number + "&ref_ids="

            headers = {
                "Content-Type": "application/json",
                "Authorization": (token if token != "" else Delhivery.Token),
            }
            # logger.info("Api Ready to post %s", api_url)

            response = requests.request(
                "GET",
                api_url,
                headers=headers,
            )

            try:
                response_data = response.json()
            except ValueError as e:
                logger.error("Delhivery Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # If tracking failed, return message
            if "Error" in response_data:
                logger.error(
                    "Delhivery track_shipment if response is not true: %s",
                    response_data,
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data,
                )
            tracking_data = response_data.get("ShipmentData", "")
            logger.info(
                "Delhivery track_shipment api response: %s",
                response_data,
            )

            # if tracking_data is not present in the respnse
            if not tracking_data:
                print(5)
                logger.error(
                    "Delhivery track_shipment if tracking_data is not true: %s",
                    tracking_data,
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            updated_awb_number = tracking_data[0]["Shipment"]["AWB"]
            courier_status = tracking_data[0]["Shipment"]["Status"]["Status"]
            activites = tracking_data[0]["Shipment"]["Scans"]

            db = get_db_session()

            old_status = order.status
            old_sub_status = order.sub_status
            # update the order status, and awb if different

            order.status = (
                status_mapping[tracking_data[0]["Shipment"]["Status"]["StatusType"]][
                    tracking_data[0]["Shipment"]["Status"]["StatusCode"]
                ].get("status", None)
                or old_status
            )
            order.sub_status = (
                status_mapping[tracking_data[0]["Shipment"]["Status"]["StatusType"]][
                    tracking_data[0]["Shipment"]["Status"]["StatusCode"]
                ].get("sub_status", None)
                or old_sub_status
            )

            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            edd = (
                tracking_data[0].get("Shipment", {}).get("ExpectedDeliveryDate")
                if tracking_data and isinstance(tracking_data[0], dict)
                else None
            )
            edd = convert_ist_to_utc(Delhivery.date_formatter(edd)) if edd else None

            pickup_completion_date = (
                tracking_data[0].get("Shipment", {}).get("PickedupDate")
                if tracking_data and isinstance(tracking_data[0], dict)
                else None
            )
            pickup_completion_date = (
                convert_ist_to_utc(Delhivery.date_formatter(pickup_completion_date))
                if edd
                else None
            )

            order.edd = edd
            order.pickup_completion_date = pickup_completion_date

            # update the tracking info
            if activites:
                new_tracking_info = []
                for activity in activites:
                    try:
                        new_tracking_info.append(
                            {
                                "status": status_mapping[
                                    activity["ScanDetail"]["ScanType"]
                                ][activity["ScanDetail"]["StatusCode"]].get(
                                    "sub_status", ""
                                )
                                or "",
                                "description": activity["ScanDetail"]["Instructions"],
                                "subinfo": activity["ScanDetail"]["Scan"],
                                "datetime": parse_datetime(
                                    Delhivery.date_formatter(
                                        activity["ScanDetail"]["StatusDateTime"]
                                    )
                                ).strftime("%d-%m-%Y %H:%M:%S"),
                                "location": activity["ScanDetail"]["ScannedLocation"],
                            }
                        )
                    except Exception as e:
                        # Log the error if needed
                        print(f"Skipping activity due to error: {e}")
                        pass

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
