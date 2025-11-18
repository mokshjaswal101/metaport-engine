import http
from psycopg2 import DatabaseError
from utils.jwt_token_handler import JWTHandler
from utils.password_hasher import PasswordHasher
from context_manager.context import context_user_data
from fastapi.responses import Response
from sqlalchemy.orm import joinedload

from starlette.responses import StreamingResponse, Response
from typing import Any
import base64

from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from schema.base import GenericResponseModel
from .easyecom_schema import (
    AuthInsertModel,
    ShippingInsertModel,
    CancelShipmentModel,
    EasyEcomAccessToken,
    UpdateTrackingStatus,
)

# from model.easycom import Easycom
from models import User, Order
from datetime import datetime, timedelta
from logger import logger
import json
from context_manager.context import get_db_session
from modules.orders.order_service import OrderService
from modules.shipment.shipment_service import ShipmentService
from modules.serviceability import ServiceabilityService
from modules.orders.order_schema import Order_create_request_model
from models import Pickup_Location
from modules.shipment.shipment_schema import CreateShipmentModel
from modules.documents.shipping_label.shipping_label_service import ShippingLabelService
import requests
import os


class TempModel(BaseModel):
    client_id: int


class EasyEcomService:

    ERROR_INVALID_CREDENTIALS = "Invalid credentials"
    ERROR_USER_NOT_FOUND = "User not found"
    EASY_COM_AUTH_PAYLOAD = "Easycom authentication payload"
    EASY_COM_SUCCESS_MESSAGE = "Successful"
    EASY_COM_INTERNAL_SERVER_ERRPR = "Internal Server Error"
    EASY_COM_SHIPPMEN_SUCCESSFULL_MESSAGE = "shipment saved successfully"
    USER_TOKEN_EXPIRE_TIME = 30  # 30 MINUTES

    @staticmethod
    def authenticate(auth_data: AuthInsertModel):
        try:
            jsonable_payload = jsonable_encoder(auth_data)

            logger.info(
                extra=context_user_data.get(),
                msg=EasyEcomService.EASY_COM_AUTH_PAYLOAD
                + "{}".format(str(jsonable_payload)),
            )

            with get_db_session() as db:

                active_User_Responce = (
                    db.query(User)
                    .filter(User.email == jsonable_payload["username"])
                    .first()
                )

                if not active_User_Responce:
                    logger.info(
                        extra=context_user_data.get(),
                        msg=EasyEcomService.ERROR_USER_NOT_FOUND
                        + "{}".format(str(jsonable_payload)),
                    )
                    return {
                        "code": http.HTTPStatus.NOT_FOUND,
                        "message": EasyEcomService.ERROR_USER_NOT_FOUND,
                    }

                if not PasswordHasher.verify_password(
                    jsonable_payload["password"], active_User_Responce.password_hash
                ):
                    logger.error(
                        extra=context_user_data.get(),
                        msg=EasyEcomService.ERROR_INVALID_CREDENTIALS,
                    )
                    return {
                        "code": http.HTTPStatus.UNAUTHORIZED,
                        "message": EasyEcomService.ERROR_INVALID_CREDENTIALS,
                    }

                if "eeApiToken" in jsonable_payload:
                    ee_api_token = jsonable_payload["eeApiToken"]

                    if ee_api_token != active_User_Responce.extra_credentials:
                        active_User_Responce.extra_credentials = ee_api_token

                        db.add(active_User_Responce)
                        db.commit()

            # token = JWTHandler.create_access_token(jsonable_encoder(ee_api_token))

            # logger.info(
            #     extra=context_user_data.get(),
            #     msg=EasyEcomService.EASY_COM_SUCCESS_MESSAGE + "=>" + token,
            # )

            return {
                "code": http.HTTPStatus.OK,
                "message": EasyEcomService.EASY_COM_SUCCESS_MESSAGE,
            }

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR
                + "{}".format(str(e)),
            )

            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR,
            }, http.HTTPStatus.INTERNAL_SERVER_ERROR

    @staticmethod
    def createShipment(shipment_data: Any):
        try:
            json_request = shipment_data
            request = json_request["order_data"]

            print(request)

            # print("request", request)

            check_user_exist = User.get_active_user_by_email(
                json_request["credentials"]["username"]
            )

            if not check_user_exist:
                logger.info(
                    extra=context_user_data.get(),
                    msg=EasyEcomService.ERROR_USER_NOT_FOUND
                    + "payload=> {}".format(str(json_request["credentials"])),
                )
                return {
                    "code": http.HTTPStatus.NOT_FOUND,
                    "message": EasyEcomService.ERROR_USER_NOT_FOUND,
                }

            if not PasswordHasher.verify_password(
                json_request["credentials"]["password"],
                jsonable_encoder(check_user_exist)["password_hash"],
            ):
                logger.error(
                    extra=context_user_data.get(),
                    msg=EasyEcomService.ERROR_INVALID_CREDENTIALS
                    + "{}"
                    + "request=>"
                    + json_request["credentials"]["password"]
                    + " , checked with =>"
                    + jsonable_encoder(check_user_exist)["password_hash"],
                )

                return {
                    "code": http.HTTPStatus.NOT_FOUND,
                    "message": EasyEcomService.ERROR_INVALID_CREDENTIALS,
                }

            client_id = check_user_exist.client_id
            context_user_data.set(TempModel(**{"client_id": client_id}))

            with get_db_session() as db:

                body = {
                    "consignee_full_name": request["customer_name"],
                    "consignee_phone": (request["contact_num"]),
                    "consignee_email": request["email"],
                    "consignee_address": request["address_line_1"],
                    "consignee_landmark": request["address_line_2"],
                    "consignee_pincode": request["pin_code"],
                    "consignee_city": request["city"],
                    "consignee_country": "India",
                    "consignee_state": request["state"],
                    "order_id": request["reference_code"],
                    "order_date": request["order_date"],
                    "channel": "easyecom",
                    "billing_is_same_as_consignee": True,
                    "products": [
                        {
                            "name": product["productName"],
                            "sku_code": product["sku"],
                            "quantity": product["item_quantity"],
                            "unit_price": product["mrp"],
                        }
                        for product in request["order_items"]
                    ],
                    "payment_mode": (
                        "prepaid" if request["payment_mode_id"] == 5 else "COD"
                    ),
                    "total_amount": request["total_amount"],
                    "order_value": sum(
                        float(product["item_quantity"]) * float(product["mrp"])
                        for product in request["order_items"]
                    ),
                    "client_id": client_id,
                    "source": "easyecom",
                    "marketplace_order_id": request["invoice_id"],
                    "status": "new",
                    "sub_status": "new",
                    # wights and dimensions
                    "length": request["Package Length"] or 10,
                    "breadth": request["Package Width"] or 10,
                    "height": request["Package Height"] or 10,
                    "company_id": 1,
                    "order_type": "B2C",
                    "discount": 0,
                    "tax_amount": request["total_tax"],
                    "shipping_charges": 0,
                    "tracking_info": [],
                    "action_history": [],
                }

                print("body", body)
                print("client_id")

                if client_id == 19:

                    body["pickup_location_code"] = "0269"

                if client_id == 2:

                    body["pickup_location_code"] = "0063"

                if client_id == 119:

                    body["pickup_location_code"] = "0348"

                if client_id == 310:

                    body["pickup_location_code"] = "0673"

                weight = request["Package Weight"]
                if not weight:
                    weight = sum(
                        product["weight"] for product in request["order_items"]
                    )

                body["weight"] = round(weight / 1000, 3)

                volumetric_weight = round(
                    (body["length"] * body["breadth"] * body["height"]) / 5000,
                    3,
                )
                applicable_weight = round(max(body["weight"], volumetric_weight), 3)
                body["volumetric_weight"] = volumetric_weight
                body["applicable_weight"] = applicable_weight

                # calc product quantity
                body["product_quantity"] = sum(
                    product["quantity"] for product in body["products"]
                )

                pickup_pincode: int = (
                    db.query(Pickup_Location.pincode)
                    .filter(
                        Pickup_Location.location_code == body["pickup_location_code"]
                    )
                    .first()
                )[0]

                zone_data = ShipmentService.calculate_shipping_zone(
                    pickup_pincode, body["consignee_pincode"]
                )

                body["zone"] = zone_data.data.get("zone", "D")

                new_order = (
                    db.query(Order)
                    .filter(
                        Order.order_id == str(body["order_id"]),
                        Order.client_id == client_id,
                    )
                    .first()
                )

                if not new_order:
                    new_order = Order.create_db_entity(body)
                    db.add(new_order)
                    db.commit()

                response = None

                if client_id == 310:

                    courier = "Bluedart Surface"
                    shipment = ShipmentService.assign_awb(
                        CreateShipmentModel(
                            order_id=new_order.order_id, contract_id=3679
                        )
                    )

                    awb = shipment.data.get("awb_number", None)
                    error_message = (
                        shipment.message
                        if hasattr(shipment, "message")
                        else "Could not post shipment due to carrier issue. Please try again later."
                    )

                    if awb is None:
                        shipment = ShipmentService.assign_awb(
                            CreateShipmentModel(
                                order_id=new_order.order_id, contract_id=4058
                            )
                        )

                        courier = "DTDC Air"
                        awb = shipment.data.get("awb_number", None)
                        # Update error message with the latest attempt's response
                        if awb is None:
                            error_message = (
                                shipment.message
                                if hasattr(shipment, "message")
                                else error_message
                            )

                    # Check if AWB was successfully assigned after all attempts
                    if awb is None:
                        logger.error(
                            extra=context_user_data.get(),
                            msg=f"AWB not assigned for order_id: {new_order.order_id}, client_id: {client_id}. Error: {error_message}",
                        )
                        return {
                            "code": http.HTTPStatus.BAD_REQUEST,
                            "message": error_message,
                            "tracking_number": None,
                            "courier_name": None,
                            "label_url": None,
                        }

                    return {
                        "code": http.HTTPStatus.OK,
                        "message": EasyEcomService.EASY_COM_SUCCESS_MESSAGE,
                        "tracking_number": awb,
                        "courier_name": courier,
                        "label_url": "https://api.lastmiles.co/api/v1/easyecom/label/"
                        + str(awb),
                    }

                else:
                    logger.error(
                        extra=context_user_data.get(),
                        msg=f"Shipment creation not supported for client_id: {client_id}",
                    )
                    return {
                        "code": http.HTTPStatus.BAD_REQUEST,
                        "message": f"Shipment creation not supported for client_id: {client_id}",
                    }

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR
                + "{}".format(str(e)),
            )

            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR,
            }

    @staticmethod
    def cancelShipment(shipment_cancel_data: CancelShipmentModel):
        try:
            shipment_dump_data = shipment_cancel_data.model_dump()
            check_user_exist = User.get_active_user_by_email(
                shipment_dump_data["credentials"]["username"]
            )

            if not check_user_exist:
                logger.info(
                    extra=context_user_data.get(),
                    msg=EasyEcomService.ERROR_USER_NOT_FOUND
                    + " , payload=> {}".format(str(shipment_dump_data)),
                )

                return {
                    "code": http.HTTPStatus.NOT_FOUND,
                    "message": EasyEcomService.ERROR_USER_NOT_FOUND,
                }

            if PasswordHasher.verify_password(
                shipment_dump_data["credentials"]["password"],
                jsonable_encoder(check_user_exist)["password_hash"],
            ):
                check_user_exist = Order.cancelled_shipment_action(
                    shipment_dump_data["awb_details"]
                )
                logger.info(
                    extra=context_user_data.get(),
                    msg=check_user_exist["message"]
                    + ",  payload=> {} ".format(str(shipment_dump_data)),
                )
                return check_user_exist
            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg=EasyEcomService.ERROR_INVALID_CREDENTIALS
                    + "{}".format(str(shipment_dump_data)),
                )

                return {
                    "code": http.HTTPStatus.NOT_FOUND,
                    "message": EasyEcomService.ERROR_INVALID_CREDENTIALS,
                }

        except DatabaseError as e:

            logger.error(
                extra=context_user_data.get(),
                msg=EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR
                + "{}".format(str(e)),
            )

            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR,
            }

    @staticmethod
    def EasyEcomAccessToken(easyEcom_acess_token: EasyEcomAccessToken):
        try:
            access_token_dump_data = easyEcom_acess_token.model_dump()
            access_token_dump_data["username"] = access_token_dump_data["email"]
            check_user_exist = User.get_active_user_by_email(
                access_token_dump_data["username"]
            )

            if not check_user_exist:
                logger.error(
                    extra=context_user_data.get(),
                    msg=EasyEcomService.ERROR_USER_NOT_FOUND,
                )

                return {
                    "code": http.HTTPStatus.NOT_FOUND,
                    "message": EasyEcomService.ERROR_USER_NOT_FOUND,
                }

            if PasswordHasher.verify_password(
                access_token_dump_data["password"],
                jsonable_encoder(check_user_exist)["password_hash"],
            ):

                token = JWTHandler.create_access_token(
                    jsonable_encoder(check_user_exist)
                )
                return {
                    "data": {
                        "companyname": "Pune",
                        "all_location": 1,
                        "time_zone": "Asia/Kolkata",
                        "logo_url": None,
                        "brandname": None,
                        "brand_id": None,
                        "logo": None,
                        "credit_limit": None,
                        "credit_balance": None,
                        "userName": jsonable_encoder(check_user_exist)["first_name"]
                        + " "
                        + jsonable_encoder(check_user_exist)["last_name"],
                        "paymentKey": 0,
                        "serialMode": False,
                        "token": {
                            "jwt_token": token,
                            "token_type": "bearer",
                            "expires_in": timedelta(
                                minutes=EasyEcomService.USER_TOKEN_EXPIRE_TIME
                            ),
                        },
                    },
                    "message": None,
                }
            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg=EasyEcomService.ERROR_INVALID_CREDENTIALS,
                )
                return {
                    "code": http.HTTPStatus.NOT_FOUND,
                    "message": EasyEcomService.ERROR_INVALID_CREDENTIALS,
                }
        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR
                + "{}".format(str(e)),
            )
            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR,
            }

    @staticmethod
    def generate_label(awb: str):
        try:

            with get_db_session() as db:
                shipment = (
                    db.query(Order)
                    .filter(Order.awb_number == awb, Order.source == "easyecom")
                    .options(joinedload(Order.pickup_location))
                    .first()
                )

                if not shipment:
                    return {
                        "code": http.HTTPStatus.NOT_FOUND,
                        "message": "Shipment not found",
                    }

                client_id = shipment.client_id
                context_user_data.set(TempModel(**{"client_id": client_id}))

                resp = ShippingLabelService.generate_label(
                    order_ids=[shipment.order_id]
                )

                # pdf_bytes = base64.b64decode(label)

                if isinstance(resp, StreamingResponse):
                    # ensure correct headers & media type
                    resp.headers["Content-Disposition"] = (
                        'attachment; filename="label.pdf"'
                    )
                    resp.media_type = "application/pdf"
                    return resp

                # Fallback: bytes/base64 handling (see Option B)
                if isinstance(resp, (bytes, bytearray)):
                    pdf_bytes = bytes(resp)
                elif isinstance(resp, str):
                    pdf_bytes = base64.b64decode(resp)
                else:
                    raise RuntimeError(f"Unsupported label type: {type(resp)}")

                return Response(
                    content=pdf_bytes,
                    media_type="application/pdf",
                    headers={"Content-Disposition": 'attachment; filename="label.pdf"'},
                )

        except DatabaseError as e:

            logger.error(
                extra=context_user_data.get(),
                msg=EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR
                + "{}".format(str(e)),
            )

            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR,
            }

    @staticmethod
    def update_order_status_to_easyecom(order: Order, user_credentials: dict = None):
        """
        Update order status back to EasyEcom without affecting existing flow.

        Args:
            order: Order model instance with current status
            user_credentials: Optional user credentials dict with eeApiToken

        Returns:
            dict: Response indicating success/failure of status update
        """
        try:
            # Get user credentials if not provided

            print("inside easyecom")

            if order.status.lower() != "delivered":
                return

            if not user_credentials:
                with get_db_session() as db:
                    user = (
                        db.query(User).filter(User.client_id == order.client_id).first()
                    )

                    if not user or not user.extra_credentials:
                        logger.warning(
                            extra=context_user_data.get(),
                            msg=f"No EasyEcom API token found for client_id: {order.client_id}",
                        )
                        return {
                            "code": http.HTTPStatus.BAD_REQUEST,
                            "message": "EasyEcom API token not found",
                        }

                    ee_api_token = user.extra_credentials
            else:
                ee_api_token = user_credentials.get("eeApiToken")

            print(ee_api_token)

            # Map internal order status to EasyEcom status
            status_mapping = {
                "new": 1,
                "booked": 2,
                "picked up": 3,
                "in transit": 4,
                "out for delivery": 5,
                "delivered": 6,
                "cancelled": 7,
                "rto": 8,
                "rto delivered": 9,
            }

            easyecom_status_id = status_mapping.get(order.status.lower(), 1)

            status = [18, 19, 2, 20, 3]

            # Prepare the payload for EasyEcom status update

            for status_id in status:
                payload = {
                    "current_shipment_status_id": status_id,
                    "awb": order.awb_number or "",
                    "estimated_delivery_date": (
                        order.edd.isoformat() if order.edd else ""
                    ),
                    "delivery_date": (
                        order.delivered_date.isoformat() if order.delivered_date else ""
                    ),
                }

                print(payload)

                # EasyEcom API endpoint (this would need to be configured based on EasyEcom's actual API)
                # Note: Replace with actual EasyEcom status update endpoint
                easyecom_api_base = os.environ.get(
                    "EASYECOM_API_BASE_URL", "https://api.easyecom.io"
                )
                update_url = f"{easyecom_api_base}/Carrier/V2/updateTrackingStatus"

                headers = {
                    "x-api-key": "534b1ebf383531ba1a1ab5dfbe4c78bd3fdde211",
                    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczpcL1wvbG9hZGJhbGFuY2VyLW0uZWFzeWVjb20uaW9cL2FjY2Vzc1wvdG9rZW4iLCJpYXQiOjE3NTcwMTY5MjksImV4cCI6MTc2NDkwMDkyOSwibmJmIjoxNzU3MDE2OTI5LCJqdGkiOiI4cG5SMERRa2lCRUd6MXZOIiwic3ViIjoyNTc4NjYsInBydiI6ImE4NGRlZjY0YWQwMTE1ZDVlY2NjMWY4ODQ1YmNkMGU3ZmU2YzRiNjAiLCJ1c2VyX2lkIjoyNTc4NjYsImNvbXBhbnlfaWQiOjE2NTI0Nywicm9sZV90eXBlX2lkIjoyLCJwaWlfYWNjZXNzIjoxLCJwaWlfcmVwb3J0X2FjY2VzcyI6MSwicm9sZXMiOm51bGwsImNfaWQiOjE2NTI0NywidV9pZCI6MjU3ODY2LCJsb2NhdGlvbl9yZXF1ZXN0ZWRfZm9yIjoxNjUyNDd9.yeYA1zdMAWcT1E-v_qgkszXNd2gZ0d_rxshnJyi8W60",
                    "Content-Type": "application/json",
                }

                # Make the API call to EasyEcom
                response = requests.post(
                    update_url, json=payload, headers=headers, timeout=30
                )

                print(response.json())

            if response.status_code == 200:
                logger.info(
                    extra=context_user_data.get(),
                    msg=f"Successfully updated order status to EasyEcom for order: {order.order_id}",
                )
                return {
                    "code": http.HTTPStatus.OK,
                    "message": "Status updated successfully to EasyEcom",
                }
            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Failed to update status to EasyEcom. Status: {response.status_code}, Response: {response.text}",
                )
                return {
                    "code": http.HTTPStatus.BAD_REQUEST,
                    "message": f"EasyEcom API error: {response.status_code}",
                }

        except requests.exceptions.RequestException as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Network error updating status to EasyEcom: {str(e)}",
            )
            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": "Network error communicating with EasyEcom",
            }

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unexpected error updating status to EasyEcom: {str(e)}",
            )
            return {
                "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": EasyEcomService.EASY_COM_INTERNAL_SERVER_ERRPR,
            }
