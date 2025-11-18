import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
import pytz
from datetime import datetime
import unicodedata
from pydantic import BaseModel
from dateutil.parser import parse

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re

# models
from models import (
    Pickup_Location,
    Order,
    Client,
    Company_Contract,
    Pincode_Serviceability,
)

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel
from modules.company_contract.company_contract_schema import CompanyContractModel

# data
from .status_mapping import status_mapping

# service
from modules.wallet.wallet_service import WalletService


from utils.datetime import parse_datetime


def clean_text(text):
    if text is None:
        return ""
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", "").strip()
    # Keep only letters and numbers, remove all other characters
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    return text


class TempModel(BaseModel):
    client_id: int


class Ekart:

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
    def Add_Contract_Generate_Token(credentials: Dict[str, str]):

        try:
            print("welcome to ekart action new")
            api_url = "https://api.ekartlogistics.com/auth/token"
            print(f"Basic {credentials.get('token')}", "Hello abcd")
            headers = {
                "Authorization": f"Basic {credentials.get('token')}",
                "Content-Type": "application/json",
                "HTTP_X_MERCHANT_CODE": credentials.get("client_code"),
            }

            response = requests.post(api_url, headers=headers, verify=False)
            data = response.json()  # convert response to dict
            print(data, "<<data>>")
            if "unauthorised" in data:
                return {
                    "status_code": http.HTTPStatus.BAD_REQUEST,
                    "status": False,
                    "message": f"unauthorised: {data.get('unauthorised')}",
                }
            else:
                return {
                    "status_code": 200,
                    "status": True,
                    "message": "Token is valid",
                }
            # response.raise_for_status()
            # response = response.json()
            print(data, "||<<response>>||")
            # print(response, "dddddddddd")
            # if response.get("failed", ""):
            #     return GenericResponseModel(
            #         status_code=http.HTTPStatus.BAD_REQUEST,
            #         message=response["failed"],
            #     )

            # if response.get("unauthorised", ""):
            #     return GenericResponseModel(
            #         status_code=http.HTTPStatus.BAD_REQUEST,
            #         message=response["unauthorised"],
            #     )

            # if response.get("forbidden", ""):
            #     return GenericResponseModel(
            #         status_code=http.HTTPStatus.BAD_REQUEST,
            #         message=response["forbidden"],
            #     )

            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=True,
                message="success",
                # data=response.get("Authorization"),
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

            api_url = "https://api.ekartlogistics.com/auth/token"

            headers = {
                "Authorization": credentials["token"],
                "Content-Type": "application/json",
                "HTTP_X_MERCHANT_CODE": credentials["client_code"],
            }

            response = requests.post(api_url, headers=headers, verify=False)
            response = response.json()

            if response.get("failed", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response["failed"],
                )

            if response.get("unauthorised", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response["unauthorised"],
                )

            if response.get("forbidden", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response["forbidden"],
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=True,
                message="success",
                data=response.get("Authorization"),
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
        company_contract: CompanyContractModel,
    ):

        try:

            client_id = context_user_data.get().client_id

            token = Ekart.get_token(credentials)

            print(token)

            if token.status == False:
                return token

            else:
                token = token.data

            print(0)

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

            pickup_serviceability = pickup_serviceability.ekart_fm

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
                delivery_serviceability = delivery_serviceability.ekart_lm_prepaid
            else:
                delivery_serviceability = delivery_serviceability.ekart_lm_cod

            if delivery_serviceability == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Destination pincode not serviceable",
                )

            print(pickup_location.location_code)

            # return

            client = db.query(Client).filter(Client.id == client_id).first()

            print(client.id)

            consignee_address1 = clean_text(order.consignee_address.strip())
            consignee_landmark = clean_text(
                order.consignee_landmark.strip() if order.consignee_landmark else ""
            )

            print(1)

            contract = (
                db.query(Company_Contract)
                .filter(Company_Contract.id == company_contract.id)
                .with_for_update()
                .first()
            )

            print(2)

            if contract:
                print(3)
                tracking_series = contract.tracking_series
                new_tracking_series = tracking_series + 1

                contract.tracking_series = new_tracking_series

                payment_prefix = "P" if order.payment_mode.lower() == "prepaid" else "C"
                tracking_id = f"{credentials['client_code']}{payment_prefix}{new_tracking_series:010d}"

            print(tracking_id)

            body = {
                "client_name": credentials["client_code"],
                "goods_category": "ESSENTIAL",
                "services": [
                    {
                        "service_code": "ECONOMY",
                        "service_details": [
                            {
                                "service_leg": "FORWARD",
                                "service_data": {
                                    "vendor_name": client.client_name,
                                    "amount_to_collect": (
                                        0
                                        if order.payment_mode.lower() == "prepaid"
                                        else float(order.total_amount)
                                    ),
                                    "payment_channel": (
                                        "PREPAID"
                                        if order.payment_mode.lower() == "prepaid"
                                        else "COD"
                                    ),
                                    "source": {
                                        "address": {
                                            "first_name": pickup_location.contact_person_name,
                                            "address_line1": pickup_location.address,
                                            "address_line2": pickup_location.landmark,
                                            "pincode": pickup_location.pincode,
                                            "city": pickup_location.city,
                                            "state": pickup_location.state,
                                            "primary_contact_number": pickup_location.contact_person_phone,
                                            "email_id": pickup_location.contact_person_email,
                                        }
                                    },
                                    "destination": {
                                        "address": {
                                            "first_name": order.consignee_full_name,
                                            "address_line1": consignee_address1,
                                            "address_line2": consignee_landmark,
                                            "pincode": order.consignee_pincode,
                                            "city": order.consignee_city,
                                            "state": order.consignee_state,
                                            "primary_contact_number": order.consignee_phone,
                                            "email_id": order.consignee_email,
                                        }
                                    },
                                    "return_location": {
                                        "address": {
                                            "first_name": pickup_location.contact_person_name,
                                            "address_line1": pickup_location.address,
                                            "address_line2": pickup_location.landmark,
                                            "pincode": pickup_location.pincode,
                                            "city": pickup_location.city,
                                            "state": pickup_location.state,
                                            "primary_contact_number": pickup_location.contact_person_phone,
                                            "email_id": pickup_location.contact_person_email,
                                        }
                                    },
                                },
                                "shipment": {
                                    "client_reference_id": "lm_"
                                    + str(order.id)
                                    + (
                                        f"/{str(order.cancel_count)}"
                                        if order.cancel_count > 0
                                        else ""
                                    ),
                                    "tracking_id": tracking_id,
                                    "shipment_value": float(order.total_amount),
                                    "shipment_dimensions": {
                                        "length": {"value": float(order.length)},
                                        "breadth": {"value": float(order.breadth)},
                                        "height": {"value": float(order.height)},
                                        "weight": {"value": float(order.weight)},
                                    },
                                    "shipment_items": [
                                        {
                                            "product_title": product["name"],
                                            "quantity": product["quantity"],
                                            "cost": {
                                                "total_sale_value": float(
                                                    product["quantity"]
                                                )
                                                * float(product["unit_price"]),
                                            },
                                            "item_attributes": [
                                                {
                                                    "name": "order_id",
                                                    "value": str(order.id)
                                                    + (
                                                        f"/{str(order.cancel_count)}"
                                                        if order.cancel_count > 0
                                                        else ""
                                                    ),
                                                },
                                                {
                                                    "name": "invoice_id",
                                                    "value": str(order.id)
                                                    + (
                                                        f"/{str(order.cancel_count)}"
                                                        if order.cancel_count > 0
                                                        else ""
                                                    ),
                                                },
                                            ],
                                        }
                                        for product in order.products
                                    ],
                                },
                            }
                        ],
                    }
                ],
            }

            print(body)

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
                "HTTP_X_MERCHANT_CODE": credentials["client_code"],
            }

            print(headers)

            api_url = "https://api.ekartlogistics.com/v2/shipments/create"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                contract.tracking_series = new_tracking_series - 1
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response_data.get("failed", ""):
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["failed"],
                )

            if response_data.get("unauthorised", ""):
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["unauthorised"],
                )

            if response_data.get("forbidden", ""):
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["forbidden"],
                )

            # if order created successfully at shiperfecto
            if response.status_code != 200:
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=", ".join(response_data["response"][0]["message"]),
                )

            if response.status_code == 200:

                response_data["response"] = response_data["response"][0]

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = response_data["response"]["tracking_id"]
                order.aggregator = "ekart"
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + "ekart",
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }

                # update the activity

                order.action_history.append(new_activity)

                db.add(order)
                db.flush()

                contract.tracking_series = new_tracking_series
                db.add(contract)
                db.flush()
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["response"]["tracking_id"],
                        "delivery_partner": delivery_partner.slug,
                    },
                    message="AWB assigned successfully",
                )

            else:
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["order_data"]["error"],
                )

        except DatabaseError as e:
            # Log database error
            contract.tracking_series = new_tracking_series - 1
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

            contract.tracking_series = new_tracking_series - 1
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
    def dev_create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
        company_contract: CompanyContractModel,
    ):

        try:

            client_id = context_user_data.get().client_id
            token = "Basic d2FyZWhvdXNpdHk6ZHVtbXlLZXk="

            # get the location code for shiperfecto from the db

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )

            client = db.query(Client).filter(Client.id == client_id).first()

            consignee_address1 = clean_text(order.consignee_address.strip())
            consignee_landmark = clean_text(order.consignee_landmark.strip())

            contract = (
                db.query(Company_Contract)
                .filter(Company_Contract.id == company_contract.id)
                .with_for_update()
                .first()
            )

            if contract:
                tracking_series = contract.tracking_series
                new_tracking_series = tracking_series + 1

                contract.tracking_series = new_tracking_series

                payment_prefix = "P" if order.payment_mode.lower() == "prepaid" else "C"
                tracking_id = f"{'WAR'}{payment_prefix}{new_tracking_series:010d}"

            print(tracking_id)

            body = {
                "client_name": "WAR",
                "goods_category": "ESSENTIAL",
                "services": [
                    {
                        "service_code": "ECONOMY",
                        "service_details": [
                            {
                                "service_leg": "FORWARD",
                                "service_data": {
                                    "vendor_name": client.client_name,
                                    "amount_to_collect": (
                                        0
                                        if order.payment_mode.lower() == "prepaid"
                                        else float(order.total_amount)
                                    ),
                                    "payment_channel": (
                                        "PREPAID"
                                        if order.payment_mode.lower() == "prepaid"
                                        else "COD"
                                    ),
                                    "source": {
                                        "address": {
                                            "first_name": pickup_location.contact_person_name,
                                            "address_line1": pickup_location.address,
                                            "address_line2": pickup_location.landmark,
                                            "pincode": pickup_location.pincode,
                                            "city": pickup_location.city,
                                            "state": pickup_location.state,
                                            "primary_contact_number": pickup_location.contact_person_phone,
                                            "email_id": pickup_location.contact_person_email,
                                        }
                                    },
                                    "destination": {
                                        "address": {
                                            "first_name": order.consignee_full_name,
                                            "address_line1": consignee_address1,
                                            "address_line2": consignee_landmark,
                                            "pincode": order.consignee_pincode,
                                            "city": order.consignee_city,
                                            "state": order.consignee_state,
                                            "primary_contact_number": order.consignee_phone,
                                            "email_id": order.consignee_email,
                                        }
                                    },
                                    "return_location": {
                                        "address": {
                                            "first_name": pickup_location.contact_person_name,
                                            "address_line1": pickup_location.address,
                                            "address_line2": pickup_location.landmark,
                                            "pincode": pickup_location.pincode,
                                            "city": pickup_location.city,
                                            "state": pickup_location.state,
                                            "primary_contact_number": pickup_location.contact_person_phone,
                                            "email_id": pickup_location.contact_person_email,
                                        }
                                    },
                                },
                                "shipment": {
                                    "client_reference_id": "LMO/"
                                    + str(client_id)
                                    + "/"
                                    + clean_text(order.order_id),
                                    "tracking_id": tracking_id,
                                    "shipment_value": float(order.total_amount),
                                    "shipment_dimensions": {
                                        "length": {"value": float(order.length)},
                                        "breadth": {"value": float(order.breadth)},
                                        "height": {"value": float(order.height)},
                                        "weight": {"value": float(order.weight)},
                                    },
                                    "shipment_items": [
                                        {
                                            "product_title": product["name"],
                                            "quantity": product["quantity"],
                                            "cost": {
                                                "total_sale_value": product["quantity"]
                                                * product["unit_price"],
                                            },
                                            "item_attributes": [
                                                {
                                                    "name": "order_id",
                                                    "value": "LMO/"
                                                    + str(client_id)
                                                    + "/"
                                                    + order.order_id,
                                                },
                                                {
                                                    "name": "invoice_id",
                                                    "value": "LMI/INV/"
                                                    + str(client_id)
                                                    + "/"
                                                    + order.order_id,
                                                },
                                            ],
                                        }
                                        for product in order.products
                                    ],
                                },
                            }
                        ],
                    }
                ],
            }

            # print(body)

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
                "HTTP_X_MERCHANT_CODE": "WAR",
            }

            # print(headers)
            print("header successf fully completed")
            # return
            # api_url = "https://api.ekartlogistics.com/v2/shipments/create"
            api_url = "https://staging.ekartlogistics.com/v2/shipments/create"

            response = requests.post(api_url, json=body, headers=headers, timeout=10)
            response_data = response.json()
            print(response_data, "||response_data||")
            try:
                response_data = response.json()
                print(response_data, "hello 123")

            except ValueError as e:
                contract.tracking_series = new_tracking_series - 1
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response_data.get("failed", ""):
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["failed"],
                )

            if response_data.get("unauthorised", ""):
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["unauthorised"],
                )

            if response_data.get("forbidden", ""):
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["forbidden"],
                )

            # if order created successfully at shiperfecto
            if response.status_code != 200:
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=", ".join(response_data["response"][0]["message"]),
                )

            if response.status_code == 200:

                response_data["response"] = response_data["response"][0]

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = response_data["response"]["tracking_id"]
                order.aggregator = "ekart"
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + "ekart",
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }

                # update the activity

                order.action_history.append(new_activity)

                db.add(order)
                db.flush()

                contract.tracking_series = new_tracking_series
                db.add(contract)
                db.flush()
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["response"]["tracking_id"],
                        "delivery_partner": delivery_partner.slug,
                    },
                    message="AWB assigned successfully",
                )

            else:
                contract.tracking_series = new_tracking_series - 1
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["order_data"]["error"],
                )

        except DatabaseError as e:
            # Log database error
            contract.tracking_series = new_tracking_series - 1
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

            contract.tracking_series = new_tracking_series - 1
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

            if order.courier_partner == "ekart 2kg":

                credentials = {
                    "client_code": "SIT",
                    "token": "Basic V0FSOml4VTc4WlQ5VDZvV1RkN0I=",
                }

            elif order.courier_partner == "ekart 0.5kg":

                credentials = {
                    "client_code": "WAR",
                    "token": "Basic d2FyZWhvdXNpdHk6dUY+OEY5Sls2RzNFIUk1Jg==",
                }

            else:
                credentials = {
                    "client_code": "PLS",
                    "token": "Basic d2FycGxzOnE2UEExbElYM21aQXFBQzg=",
                }

            token = Ekart.get_token(credentials=credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
                "HTTP_X_MERCHANT_CODE": credentials["client_code"],
            }

            print(headers)

            body = {"tracking_ids": [awb_number]}

            api_url = "https://api.ekartlogistics.com/v2/shipments/track"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

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

            # If order creation failed at Shiperfecto, return message
            if response_data.get("failed", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["failed"],
                )

            if response_data.get("unauthorised", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["unauthorised"],
                )

            if response_data.get("forbidden", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["forbidden"],
                )

            # if order created successfully at shiperfecto
            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=", ".join(response_data["response"][0]["message"]),
                )

            tracking_data = response_data.get(awb_number, "")

            # if tracking_data is not present in the respnse
            if not tracking_data:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = tracking_data.get("history", "")[0]["status"]
            updated_awb_number = tracking_data.get("external_tracking_id", "")

            activites = tracking_data.get("history", "")

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

            edd = tracking_data.get("expected_delivery_date", None)

            if edd:
                try:
                    local_time = ist.localize(
                        datetime.strptime(edd, "%Y-%m-%d %H:%M:%S")
                    )
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
                        "description": activity.get("public_description", ""),
                        "subinfo": activity.get("hub_name", ""),
                        "datetime": parse(activity.get("event_date", "")).strftime(
                            "%d-%m-%Y %H:%M:%S"
                        ),
                        "location": activity.get("city", ""),
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

            api_url = "https://api.ekartlogistics.com/v3/shipments/rto/create"

            if order.courier_partner == "ekart 2kg":

                credentials = {
                    "client_code": "SIT",
                    "token": "Basic V0FSOml4VTc4WlQ5VDZvV1RkN0I=",
                }

            elif order.courier_partner == "ekart 0.5kg":

                credentials = {
                    "client_code": "WAR",
                    "token": "Basic d2FyZWhvdXNpdHk6dUY+OEY5Sls2RzNFIUk1Jg==",
                }

            else:
                credentials = {
                    "client_code": "PLS",
                    "token": "Basic d2FycGxzOnE2UEExbElYM21aQXFBQzg=",
                }

            body = {"awb": awb_number}

            token = Ekart.get_token(credentials=credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
                "HTTP_X_MERCHANT_CODE": credentials["client_code"],
            }

            body = {
                "request_details": {
                    "tracking_id": awb_number,
                    "reason": "Order Cancelled",
                }
            }

            response = requests.put(
                api_url, headers=headers, json=body, verify=False, timeout=10
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            # If order creation failed at Shiperfecto, return message
            if response_data.get("failed", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["failed"],
                )

            if response_data.get("unauthorised", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["unauthorised"],
                )

            if response_data.get("forbidden", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["forbidden"],
                )

            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=", ".join(response_data["response"][0]["message"]),
                )

            if response.status_code == 200:

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

            awb_number = track_req.get("vendor_tracking_id", None)

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

            if courier_status != "delivery_attempt_metadata":
                order.status = status_mapping[courier_status]["status"]
                order.sub_status = status_mapping[courier_status]["sub_status"]

            order.courier_status = courier_status

            new_tracking_info = {
                "status": status_mapping.get(courier_status, {}).get(
                    "status", courier_status
                ),
                "description": track_req.get("status", ""),
                "subinfo": track_req.get("status", ""),
                "datetime": parse(track_req.get("event_date", "")).strftime(
                    "%d-%m-%Y %H:%M:%S"
                ),
                "location": track_req.get("location", ""),
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
