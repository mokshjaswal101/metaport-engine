import http
from decimal import Decimal
import requests
from sqlalchemy import desc
from datetime import datetime, timezone
import os
from psycopg2 import DatabaseError

from context_manager.context import context_user_data, get_db_session
from logger import logger
from uuid import UUID

# models
from models import Client, Order, ShippingNotificationLogs

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model

# service
from modules.wallet_logs.wallet_logs_service import WalletLogsService


template_mapping = {
    "order_processed": "order_processing_2",
    "order_shipped": "order_shipped_2",
}


def get_product_summary(products):
    if not products:
        return ""

    first_name = products[0]["name"]
    remaining_count = len(products) - 1

    if remaining_count > 0:
        return f"{first_name} + {remaining_count}"
    else:
        return first_name


class WhatsappService:

    @staticmethod
    def send_message(order: Order_Model, notification_type: str):

        try:

            client_id = order.client_id

            db = get_db_session()

            client = db.query(Client).filter(Client.id == client_id).first()
            client_name = client.client_name

            url = "https://api.botmen.in/api/create-message"

            if notification_type == "order_processed":
                payload = {
                    "appkey": "6b352ece-27c6-4e71-a3d9-ceb9a867f924",
                    "authkey": "7pDog5bwgCX5rYKkxbURmzyK7f9s0Uw0g4Fyv1nMCRvzNztXan",
                    "to": "91" + str(order.consignee_phone),
                    "template_id": "order_processing_3",
                    "language": "en",
                    "variables[{1}]": order.consignee_full_name,
                    "variables[{2}]": client_name,
                    "variables[{3}]": order.order_id,
                }

            elif notification_type == "order_shipped":

                payload = {
                    "appkey": "6b352ece-27c6-4e71-a3d9-ceb9a867f924",
                    "authkey": "7pDog5bwgCX5rYKkxbURmzyK7f9s0Uw0g4Fyv1nMCRvzNztXan",
                    "to": "91" + str(order.consignee_phone),
                    "template_id": "order_shipped_2",
                    "language": "en",
                    "variables[{1}]": order.consignee_full_name,
                    "variables[{2}]": client_name,
                    "variables[{3}]": order.order_id,
                    "buttons[{b1_type}]": "url",
                    "buttons[{b1_value}]": order.awb_number,
                }

            headers = {}

            print(payload)

            response = requests.request("POST", url, headers=headers, data=payload)

            # print(response.text)

            return response

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error posting shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    @staticmethod
    def send_order_confirmation_message(order: Order_Model):

        try:

            client_id = order.client_id

            db = get_db_session()

            client = db.query(Client).filter(Client.id == client_id).first()
            client_name = client.client_name

            url = "https://api.botmen.in/api/create-message"

            payload = {
                "appkey": "6b352ece-27c6-4e71-a3d9-ceb9a867f924",
                "authkey": "7pDog5bwgCX5rYKkxbURmzyK7f9s0Uw0g4Fyv1nMCRvzNztXan",
                "to": "91" + str(order.consignee_phone),
                "template_id": "order_confirmation_url",
                "language": "en",
                "variables[{1}]": order.consignee_full_name,
                "variables[{2}]": client_name,
                "variables[{3}]": get_product_summary(order.products),
                "variables[{4}]": order.total_amount,
                "variables[{5}]": (
                    "Cash on Delivery"
                    if order.payment_mode.lower() == "cod"
                    else order.payment_mode
                ),
                "buttons[{b1_type}]": "url",
                "buttons[{b1_value}]": order.uuid,
                "buttons[{b2_type}]": "url",
                "buttons[{b2_value}]": order.uuid,
            }

            print(payload)

            response = requests.request("POST", url, headers={}, data=payload)

            print(response.text)

            return response

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error posting shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    @staticmethod
    def confirm_order(id: UUID):

        try:

            db = get_db_session()
            order = db.query(Order).filter(Order.uuid == id).first()

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid AWB",
                    status=False,
                )

            tags = order.order_tags or []

            # Remove 'order_cancelled' if present
            tags = [tag for tag in tags if tag != "order_cancelled"]

            if "order_confirmed" not in tags:
                tags.append("order_confirmed")

            order.order_tags = tags

            log = {
                "order_id": order.id,
                "direction": "received",
                "sent_at": datetime.now(timezone.utc),
                "message_type": "order_confirmation_confirm_response",
                "content": "Confirm Order",
                "cost": 0,
                "status": "received",
            }

            message_log = ShippingNotificationLogs(**log)
            db.add(message_log)

            db.add(order)
            db.flush()
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Successfull",
                status=True,
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
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    @staticmethod
    def cancel_order(id: UUID):

        try:

            db = get_db_session()
            order = db.query(Order).filter(Order.uuid == id).first()

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid AWB",
                    status=False,
                )

            tags = order.order_tags or []

            # Remove 'order_cancelled' if present
            tags = [tag for tag in tags if tag != "order_confirmed"]

            if "order_cancelled" not in tags:
                tags.append("order_cancelled")

            order.order_tags = tags

            log = {
                "order_id": order.id,
                "direction": "received",
                "sent_at": datetime.now(timezone.utc),
                "message_type": "order_confirmation_cancel_response",
                "content": "Cancel Order",
                "cost": 0,
                "status": "received",
            }

            message_log = ShippingNotificationLogs(**log)
            db.add(message_log)

            db.add(order)
            db.flush()
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Successfull",
                status=True,
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
                message="Unable to get balance",
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
                message="Unable to get balance",
            )
