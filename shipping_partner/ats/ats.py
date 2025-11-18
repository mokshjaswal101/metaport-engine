import http
from urllib.parse import quote


from psycopg2 import DatabaseError
from requests.exceptions import ConnectionError, Timeout, HTTPError, RequestException
from typing import Dict
import requests
from datetime import datetime
import os
import pytz
import unicodedata
from pydantic import BaseModel

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re

# models
from models import Pickup_Location, Order, Pincode_Serviceability, Shipping_Label_Files

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# data
from .status_mapping import status_mapping

# service
from modules.wallet.wallet_service import WalletService

from utils.datetime import parse_datetime


def format_location(location):
    if not location:
        return "N/A"  # Handle case when location is None

    # Extract values, replacing None with empty strings
    city = location.get("city") or ""
    state = location.get("stateOrRegion") or ""
    postal_code = location.get("postalCode") or ""
    country = location.get("countryCode") or ""

    # Create a list with only non-empty values
    formatted_parts = [part for part in [city, state, postal_code, country] if part]

    return ", ".join(formatted_parts) if formatted_parts else "N/A"


def convert_utc_to_ist(utc_time):
    if not utc_time:
        return "N/A"  # Handle empty input

    # Parse the UTC timestamp
    utc_dt = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%SZ")

    # Convert to IST
    ist_tz = pytz.timezone("Asia/Kolkata")
    ist_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(ist_tz)

    # Format the output string
    return ist_dt.strftime("%Y-%m-%d %I:%M:%S")  # Example: "2025-03-16 10:33:03 AM IST"


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


class ATS:

    def Add_Contract_Generate_Token(credentials: Dict[str, str]):
        try:
            url = "https://api.amazon.co.uk/auth/o2/token"
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": credentials["ats_client_refresh_token"],
                "client_id": credentials["ats_client_id"],
                "client_secret": credentials["ats_client_secret"],
            }
            print(payload, "<<payload>>")
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(url, headers=headers, data=payload)

            token_data = response.json()
            access_token = token_data.get("access_token")
            if not access_token:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    status=False,
                    message="Invalid",
                )

            return access_token

        except requests.exceptions.RequestException as e:
            logger.error("Request error: %s", e)
            return {"error": "Failed to generate token. Check logs for details."}

    def generate_token():
        try:
            url = "https://api.amazon.co.uk/auth/o2/token"

            payload = {
                "grant_type": "refresh_token",
                "refresh_token": os.environ.get("ATS_CLIENT_REFRESH_TOKEN"),
                "client_id": os.environ.get("ATS_CLIENT_ID"),
                "client_secret": os.environ.get("ATS_CLIENT_SECRET"),
            }

            print(payload)

            headers = {"Content-Type": "application/x-www-form-urlencoded"}

            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()

            token_data = response.json()
            access_token = token_data.get("access_token")

            if not access_token:
                raise ValueError("Access token not found in response.")

            return access_token

        except requests.exceptions.RequestException as e:
            logger.error("Request error: %s", e)
            return {"error": "Failed to generate token. Check logs for details."}

    @staticmethod
    def create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):

        try:

            client_id = context_user_data.get().client_id

            token = ATS.generate_token()

            print("Token", token)

            # return

            # get the location code for shiperfecto from the db
            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )

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

            pickup_serviceability = pickup_serviceability.ats_fm

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

            delivery_serviceability = delivery_serviceability.ats_lm

            if delivery_serviceability == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Destination pincode not serviceable",
                )

            combined_address = (
                order.consignee_address.strip()
                + " "
                + (order.consignee_landmark.strip() if order.consignee_landmark else "")
            )

            combined_pickup_adderess = (
                pickup_location.address.strip()
                + " "
                + (pickup_location.landmark.strip() if pickup_location.landmark else "")
            )

            # Split into two parts
            consignee_address1 = clean_text(combined_address[:60])
            consignee_address2 = clean_text(combined_address[60:120])
            consignee_address3 = clean_text(combined_address[120:180])

            pickup_address1 = clean_text(combined_pickup_adderess[:60])
            pickup_address2 = clean_text(combined_pickup_adderess[60:120])
            pickup_address3 = clean_text(combined_pickup_adderess[120:180])

            body = {
                "shipTo": {
                    "name": order.consignee_full_name,
                    "addressLine1": consignee_address1,
                    "addressLine2": consignee_address2,
                    "addressLine3": consignee_address3,
                    "companyName": order.consignee_company,
                    "stateOrRegion": order.consignee_city,
                    "city": order.consignee_city,
                    "countryCode": "IN",
                    "postalCode": order.consignee_pincode,
                    "email": order.consignee_email,
                    "phoneNumber": order.consignee_phone,
                },
                "shipFrom": {
                    "name": pickup_location.location_name,
                    "addressLine1": pickup_address1,
                    "addressLine2": pickup_address2,
                    "addressLine3": pickup_address3,
                    "companyName": "",
                    "stateOrRegion": pickup_location.state,
                    "city": pickup_location.city,
                    "countryCode": "IN",
                    "postalCode": pickup_location.pincode,
                    "email": pickup_location.contact_person_email,
                    "phoneNumber": pickup_location.contact_person_phone,
                },
                "returnTo": {
                    "name": pickup_location.location_name,
                    "addressLine1": pickup_address1,
                    "addressLine2": pickup_address2,
                    "addressLine3": pickup_address3,
                    "companyName": "",
                    "stateOrRegion": pickup_location.state,
                    "city": pickup_location.city,
                    "countryCode": "IN",
                    "postalCode": pickup_location.pincode,
                    "email": pickup_location.contact_person_email,
                    "phoneNumber": pickup_location.contact_person_phone,
                },
                "packages": [
                    {
                        "dimensions": {
                            "length": float(order.length),
                            "width": float(order.breadth),
                            "height": float(order.height),
                            "unit": "CENTIMETER",
                        },
                        "weight": {
                            "unit": "GRAM",
                            "value": float(order.weight * 1000),
                        },
                        "insuredValue": {
                            "value": float(order.total_amount),
                            "unit": "INR",
                        },
                        "isHazmat": False,
                        "sellerDisplayName": pickup_location.location_name,
                        "charges": [
                            {
                                "amount": {
                                    "value": float(order.tax_amount),
                                    "unit": "INR",
                                },
                                "chargeType": "TAX",
                            }
                        ],
                        "productType": "general",
                        "items": [
                            {
                                "itemValue": {
                                    "value": float(product["unit_price"]),
                                    "unit": "INR",
                                },
                                # Include quantity in the description so the generated label shows it
                                "description": f"{product['name']}, QTY:{product['quantity']}",
                                "itemIdentifier": product["sku_code"],
                                "quantity": product["quantity"],
                                "isHazmat": False,
                                "productType": "general",
                                "weight": {
                                    "unit": "GRAM",
                                    "value": (float(order.weight) * 1000)
                                    / order.product_quantity,
                                },
                            }
                            for product in order.products
                        ],
                        "packageClientReferenceId": "LMO/"
                        + str(client_id)
                        + "/"
                        + order.order_id,
                    }
                ],
                "channelDetails": {
                    "channelType": "EXTERNAL",
                },
                "serviceSelection": {
                    "serviceId": [
                        "SWA-IN-OA",
                    ]
                },
                "taxDetails": [{"taxType": "GST", "taxRegistrationNumber": "-"}],
                "channelDetails": {"channelType": "EXTERNAL"},
                "serviceSelection": {"serviceId": ["SWA-IN-OA"]},
            }

            if order.payment_mode.lower() != "prepaid":
                body["valueAddedServices"] = {
                    "collectOnDelivery": {
                        "amount": {"value": float(order.total_amount), "unit": "INR"}
                    }
                }

            print(body)

            headers = {
                "x-amz-access-token": token,
                "Content-Type": "application/json",
                "x-amzn-shipping-business-id": "AmazonShipping_IN",
            }

            api_url = (
                "https://sellingpartnerapi-eu.amazon.com/shipping/v2/shipments/rates"
            )

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

            print(response.text)

            # return

            try:
                response_data = response.json()
                print(response_data)

                # return

                if response_data.get("errors") is not None:
                    # Return a generic response without exposing details
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Could not post shipment. Please try again later.",
                    )

                request_token = response_data["payload"]["requestToken"]
                rate_id = response_data["payload"]["rates"][0]["rateId"]

                payload = {
                    "requestToken": request_token,
                    "rateId": rate_id,
                    "requestedDocumentSpecification": {
                        "format": "PDF",
                        "size": {"width": 4, "length": 6, "unit": "INCH"},
                        "dpi": 300,
                        "pageLayout": "DEFAULT",
                        "needFileJoining": False,
                        "requestedDocumentTypes": ["LABEL"],
                    },
                }

                if order.payment_mode.lower() != "prepaid":
                    payload["requestedValueAddedServices"] = [
                        {"id": "CollectOnDelivery"}
                    ]
                    payload["requestedDocumentSpecification"][
                        "requestedLabelCustomization"
                    ] = {
                        "requestAttributes": [
                            "PACKAGE_CLIENT_REFERENCE_ID",
                            "COLLECT_ON_DELIVERY_AMOUNT",
                        ]
                    }

                print("payload", payload)

                url = "https://sellingpartnerapi-eu.amazon.com/shipping/v2/shipments"

                response = requests.post(url, headers=headers, json=payload)

                print(response.text)
                response = response.json()

                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = (
                    response["payload"]["packageDocumentDetails"][0]["trackingId"],
                )
                order.aggregator = "ats"
                order.courier_partner = delivery_partner.slug

                order.shipping_partner_shipping_id = response["payload"]["shipmentId"]

                shipping_label = response["payload"]["packageDocumentDetails"][0][
                    "packageDocuments"
                ][0]["contents"]

                # check if an entry for this order already exists in the label file table

                existing_document = (
                    db.query(Shipping_Label_Files)
                    .filter(Shipping_Label_Files.order == order.id)
                    .first()
                )

                if existing_document:
                    existing_document.document = shipping_label
                else:
                    new_document = Shipping_Label_Files(
                        order=order.id, document=shipping_label
                    )
                    db.add(new_document)

                db.add(order)

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response["payload"]["packageDocumentDetails"][0][
                            "trackingId"
                        ],
                        "delivery_partner": "ATS",
                    },
                    message="AWB assigned successfully",
                )

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
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
    def generate_label(
        order: Order_Model,
    ):

        db = get_db_session()

        existing_shipping_label = (
            db.query(Shipping_Label_Files)
            .filter(Shipping_Label_Files.order == order.id)
            .first()
        )

        if existing_shipping_label:
            return existing_shipping_label.document

        client_id = context_user_data.get().client_id

        access_token = ATS.generate_token()

        safe_order_id = quote(str(order.order_id), safe="")
        url = f"https://sellingpartnerapi-eu.amazon.com/shipping/v2/shipments/{order.shipping_partner_shipping_id}/documents"
        params = {
            "packageClientReferenceId": "LMO/" + str(client_id) + "/" + safe_order_id,
            "format": "PDF",
        }
        headers = {
            "x-amz-access-token": access_token,
            "x-amzn-shipping-business-id": "AmazonShipping_IN",
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            # Raise an HTTPError for bad responses

            print(response.text)

            response = response.json()

            return response["payload"]["packageDocumentDetail"]["packageDocuments"][0][
                "contents"
            ]
        except requests.exceptions.RequestException as e:
            print(f"Error occurred: {e}")
            return None

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str, credentials=None):

        try:

            token = ATS.generate_token()

            print(1)
            print(token)
            print(2)

            api_url = (
                "https://sellingpartnerapi-eu.amazon.com/shipping/v2/tracking?carrierId=ATS&trackingId="
                + awb_number
            )

            print(3)

            headers = {
                "x-amz-access-token": token,
                "Content-Type": "application/json",
                "x-amzn-shipping-business-id": "AmazonShipping_IN",
            }

            print(4)

            response = requests.get(api_url, headers=headers, verify=False, timeout=60)

            print(5)

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

            # return

            response_data = response_data.get("payload", None)

            # If tracking failed, return message
            if response_data["eventHistory"] == None:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Could not track AWB",
                )

            activites = response_data.get("eventHistory", [])

            db = get_db_session()

            # print(courier_status)

            latest_activity = activites[-1] if activites else None
            shipment_type = latest_activity.get("shipmentType", "FORWARD")

            event_history = response_data.get("eventHistory", [])
            courier_status = event_history[-1]["eventCode"] if event_history else None
            updated_awb_number = response_data["trackingId"]

            new_status = status_mapping[shipment_type][courier_status]["status"]
            new_sub_status = status_mapping[shipment_type][courier_status]["sub_status"]

            second_last_courier_status = (
                event_history[-2]["eventCode"] if len(event_history) > 1 else None
            )
            second_last_status = (
                status_mapping[event_history[-2]["shipmentType"]][
                    second_last_courier_status
                ]["status"]
                if second_last_courier_status
                else None
            )

            print("second_last_status", second_last_status)

            if second_last_status == "delivered":
                new_status = "delivered"
                new_sub_status = "delivered"
                courier_status = "DELIVERED"

                # remove the last activetiy from list
                if activites:
                    activites.pop()

            # update the order status, and awb if different

            order.sub_status = new_sub_status
            order.status = new_status
            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            edd = (
                response_data["promisedDeliveryDate"]
                if response_data.get("promisedDeliveryDate")
                else None
            )

            order.edd = edd

            # update the tracking info
            if activites:

                new_tracking_info = [
                    {
                        "status": status_mapping[activity.get("shipmentType")][
                            activity.get("eventCode", "")
                        ]["sub_status"],
                        "description": activity.get("eventCode", ""),
                        "subinfo": status_mapping[activity.get("shipmentType")][
                            activity.get("eventCode", "")
                        ]["sub_status"],
                        "datetime": convert_utc_to_ist(activity.get("eventTime", "")),
                        "location": format_location(activity.get("location", {})),
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
    def tracking_webhook(track_req):

        try:

            from modules.shipment.shipment_service import ShipmentService

            print(track_req)

            db = get_db_session()

            awb_number = track_req["detail"]["trackingId"]

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

            tracking = track_req["detail"]

            shipment_type = tracking["shipmentType"]

            courier_status = tracking["eventCode"]

            new_status = status_mapping[shipment_type][courier_status]["status"]
            new_sub_status = status_mapping[shipment_type][courier_status]["sub_status"]

            # create the new tracking info object
            new_tracking_info = {
                "status": new_sub_status,
                "description": tracking["eventCode"],
                "subinfo": new_status,
                "datetime": convert_utc_to_ist(tracking.get("eventTime", "")),
                "location": format_location(tracking.get("location", {})),
            }

            # get the last trackign info from the order
            previous_tracking_info = (
                order.tracking_info[0] if order.tracking_info else {}
            )
            previous_tracking_status = previous_tracking_info.get("status", "")

            # check if this is the case of fake in transit update that comes after delivered / RTO delivered
            if (
                previous_tracking_status == "delivered" and new_status == "in transit"
            ) or (
                previous_tracking_status == "RTO delivered"
                and new_status == "RTO in transit"
            ):
                # If the last status was delivered or RTO delivered, do not update the order status
                pass

            else:
                order.sub_status = new_sub_status
                order.status = new_status
                order.courier_status = courier_status

            if not order.tracking_info:
                order.tracking_info = []

            order.tracking_info = [new_tracking_info] + order.tracking_info

            # update the order status, and awb if different

            edd = (
                tracking["promisedDeliveryDate"]
                if tracking.get("promisedDeliveryDate")
                else None
            )

            order.edd = edd

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
    def cancel_shipment(order: Order_Model, awb_number: str):

        client_id = context_user_data.get().client_id

        try:

            token = ATS.generate_token()

            print("Token", token)

            # return

            headers = {
                "x-amz-access-token": token,
                "Content-Type": "application/json",
                "x-amzn-shipping-business-id": "AmazonShipping_IN",
            }

            api_url = (
                "https://sellingpartnerapi-eu.amazon.com/shipping/v2/shipments/"
                + str(order.shipping_partner_shipping_id)
                + "/cancel"
            )

            print(api_url)

            response = requests.put(api_url, headers=headers, verify=False, timeout=10)

            print(response.text)

            # return

            try:
                response_data = response.json()
                print(response_data)

                # return

                if response_data.get("errors") is not None:
                    # Return a generic response without exposing details
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Could not cancel shipment. Please try again later.",
                    )

                db = get_db_session()

                existing_shipping_label = (
                    db.query(Shipping_Label_Files)
                    .filter(Shipping_Label_Files.order == order.id)
                    .first()
                )

                if existing_shipping_label:
                    db.delete(existing_shipping_label)
                    db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="order cancelled successfully",
                )

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
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
    def ndr_action(order: Order_Model, awb_number: str):

        client_id = context_user_data.get().client_id

        try:

            token = ATS.generate_token()

            print("Token", token)

            # return

            headers = {
                "x-amz-access-token": token,
                "Content-Type": "application/json",
                "x-amzn-shipping-business-id": "AmazonShipping_IN",
            }

            api_url = "https://sellingpartnerapi-eu.amazon.com/shipping/v2/ndrFeedback"

            print(api_url)

            body = {
                "trackingId": awb_number,
                "ndrAction": "REATTEMPT",
                "ndrRequestData": {
                    "additionalAddressNotes": order.consignee_address.strip()
                    + " "
                    + (
                        order.consignee_landmark.strip()
                        if order.consignee_landmark
                        else ""
                    ),
                },
            }

            response = requests.post(
                api_url, headers=headers, json=body, verify=False, timeout=10
            )

            print(response.text)

            # return

            try:
                response_data = response.json()
                print(response_data)

                # return

                if response_data.get("errors") is not None:
                    # Return a generic response without exposing details
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Could not cancel shipment. Please try again later.",
                    )

                db = get_db_session()

                existing_shipping_label = (
                    db.query(Shipping_Label_Files)
                    .filter(Shipping_Label_Files.order == order.id)
                    .first()
                )

                if existing_shipping_label:
                    db.delete(existing_shipping_label)
                    db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="order cancelled successfully",
                )

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
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
