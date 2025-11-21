import http
from decimal import Decimal
import os
from datetime import datetime
from pydantic import BaseModel
import razorpay
from psycopg2 import DatabaseError
from sqlalchemy.ext.asyncio import AsyncSession
import razorpay.errors

from context_manager.context import context_user_data, get_db_session
from logger import logger

# models
from models import Wallet, PaymentRecords

# schema
from schema.base import GenericResponseModel
from .razorpay_schema import RazorPayValidateRequest

# service
from modules.wallet.wallet_service import WalletService


class TempModel(BaseModel):
    client_id: int


class Razorpay:

    KEY_ID = os.environ.get("RAZORPAY_KEY_ID")
    SECRET_KEY = os.environ.get("RAZORPAY_SECRET_KEY")

    client = razorpay.Client(auth=(KEY_ID, SECRET_KEY))

    # @staticmethod
    # def create_order(amount: float, wallet_type: str) -> GenericResponseModel:
    #     try:

    #         client_id = context_user_data.get().client_id

    #         payload = {"amount": amount * 100, "currency": "INR"}

    #         # create a razor pay order that will give us an order id, that will then be used to complete the transaction at the frontend
    #         order = Razorpay.client.order.create(data=payload)

    #         # in case the order creation fails, return payment failed
    #         if order is None or order.get("error", ""):
    #             GenericResponseModel(
    #                 status_code=http.HTTPStatus.BAD_REQUEST,
    #                 message=(order["error"].get("description", "Payment Failed")),
    #             )

    #         recharge_type = (
    #             "wallet recharge"
    #             if wallet_type == "wallet"
    #             else "shipping notifications"
    #         )

    #         # create a payment log in the payment records table
    #         with get_db_session() as db:
    #             db.add(
    #                 PaymentRecords(
    #                     gateway="razorpay",
    #                     order_id=order["id"],
    #                     status="payment initiated",
    #                     amount=amount,
    #                     currency="INR",
    #                     type=recharge_type,
    #                     client_id=client_id,
    #                 )
    #             )
    #             db.commit()

    #         # return order data
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.OK,
    #             status=True,
    #             data={
    #                 "order_id": order["id"],
    #                 "amount": amount,
    #                 "currency": "INR",
    #                 "key": Razorpay.KEY_ID,  # Send key to frontend
    #             },
    #             message="Razorpay Payment Initiated",
    #         )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Error initiating payment: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Payment Failed.",
    #         )
    @staticmethod
    async def create_order(amount: float, wallet_type: str) -> GenericResponseModel:
        try:
            client_id = context_user_data.get().client_id
            payload = {"amount": amount * 100, "currency": "INR"}

            # create razorpay order
            order = Razorpay.client.order.create(data=payload)

            if order is None or order.get("error", ""):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=(
                        order.get("error", {}).get("description", "Payment Failed")
                    ),
                )

            recharge_type = (
                "wallet recharge"
                if wallet_type == "wallet"
                else "shipping notifications"
            )

            # create payment log asynchronously
            db: AsyncSession = get_db_session()
            try:
                db.add(
                    PaymentRecords(
                        gateway="razorpay",
                        order_id=order["id"],
                        status="payment initiated",
                        amount=amount,
                        currency="INR",
                        type=recharge_type,
                        client_id=client_id,
                    )
                )
                await db.commit()
            finally:
                await db.close()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "order_id": order["id"],
                    "amount": amount,
                    "currency": "INR",
                    "key": Razorpay.KEY_ID,
                },
                message="Razorpay Payment Initiated",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error initiating payment: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Payment Failed.",
            )

    @staticmethod
    def verify_payment(payment_req: RazorPayValidateRequest):
        try:

            with get_db_session() as db:

                payment_record = (
                    db.query(PaymentRecords)
                    .filter_by(order_id=payment_req.order_id)
                    .first()
                )

                payment_status = payment_record.status

                response_message = ""

                if payment_status == "payment authorized":
                    response_message = (
                        "Payment under process. Please check in some time"
                    )

                else:
                    response_message = payment_status

                # Return success response
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=payment_status == "payment successful",
                    message=response_message,
                )

        except Exception as e:
            # Log unexpected errors
            logger.error(
                f"Unhandled error during payment verification: {str(e)}",
                extra=context_user_data.get(),
            )

            # Update payment record as "verification error"
            with get_db_session() as db:
                payment_record = (
                    db.query(PaymentRecords)
                    .filter_by(order_id=payment_req.order_id)
                    .first()
                )
                if payment_record:
                    payment_record.status = "verification failed"
                    db.flush()

            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Payment verification failed.",
            )

    @staticmethod
    def payment_status_webhook(payment_req):
        try:
            # Extract event type and payload
            event = payment_req.get("event")
            payload = payment_req.get("payload", {}).get("payment", {}).get("entity")

            # Validate essential fields
            if not event or not payload:
                logger.error("Invalid webhook payload", extra={"payload": payment_req})
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid payload",
                )

            # Log the webhook
            logger.info(f"Received Razorpay webhook: {event}", extra={"event": event})

            # Route to specific event handlers
            if event == "payment.authorized":
                return Razorpay._handle_payment_authorized(payload)
            elif event == "payment.failed":
                return Razorpay._handle_payment_failed(payload)
            elif event == "payment.captured":
                return Razorpay._handle_payment_captured(payload)
            else:
                logger.warning(f"Unhandled webhook event: {event}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Unhandled event type: {event}",
                )

        except razorpay.errors.SignatureVerificationError:
            logger.warning(
                "Signature verification failed for webhook",
                extra={"payload": payment_req},
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                message="Signature verification failed",
            )

        except Exception as e:
            logger.error(
                f"Unhandled error during webhook processing: {str(e)}",
                extra={"payload": payment_req},
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unexpected error during webhook processing",
            )

    @staticmethod
    def _handle_payment_authorized(payload):
        with get_db_session() as db:
            order_id = payload.get("order_id")
            payment_id = payload.get("id")
            amount = payload.get("amount")
            method = payload.get("method", None)

            if not order_id or not payment_id or not amount:
                logger.error("Missing essential fields in payload", extra=payload)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Essential fields missing in payload",
                )

            # Fetch payment record
            payment_record = (
                db.query(PaymentRecords).filter_by(order_id=order_id).first()
            )
            if not payment_record:
                logger.warning(f"No payment record found for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Payment record not found",
                )

            # Avoid duplicate updates
            if (
                payment_record.status == "payment authorized"
                or payment_record.status == "payment successful"
            ):
                logger.info(f"Duplicate authorization for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Duplicate webhook ignored",
                )

            # Update payment record
            payment_record.status = "payment authorized"
            payment_record.payment_id = payment_id
            payment_record.method = method
            payment_record.amount = amount / 100  # Store amount in original currency
            db.add(payment_record)
            db.commit()

            logger.info(f"Payment authorized for order_id: {order_id}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Payment authorized successfully.",
            )

    @staticmethod
    def _handle_payment_failed(payload):
        with get_db_session() as db:
            order_id = payload.get("order_id")
            payment_id = payload.get("id")
            failure_reason = payload.get("error", {}).get(
                "description", "Unknown error"
            )

            if not order_id or not payment_id:
                logger.error("Missing essential fields in payload", extra=payload)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Essential fields missing in payload",
                )

            # Fetch payment record
            payment_record = (
                db.query(PaymentRecords).filter_by(order_id=order_id).first()
            )
            if not payment_record:
                logger.warning(f"No payment record found for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Payment record not found",
                )

            # Update payment record
            payment_record.status = "payment failed"
            payment_record.failure_reason = failure_reason
            db.add(payment_record)
            db.commit()

            logger.info(f"Payment failed for order_id: {order_id}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Payment failed.",
            )

    @staticmethod
    def _handle_payment_captured(payload):
        with get_db_session() as db:
            order_id = payload.get("order_id")
            payment_id = payload.get("id")
            amount = payload.get("amount")

            if not order_id or not payment_id or not amount:
                logger.error("Missing essential fields in payload", extra=payload)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Essential fields missing in payload",
                )

            # Fetch payment record
            payment_record = (
                db.query(PaymentRecords).filter_by(order_id=order_id).first()
            )
            if not payment_record:
                logger.warning(f"No payment record found for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Payment record not found",
                )

            # Avoid duplicate updates
            if payment_record.status == "payment successful":
                logger.info(f"Duplicate captured event for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Duplicate webhook ignored",
                )

            client_id = payment_record.client_id
            context_user_data.set(TempModel(**{"client_id": client_id}))

            if payment_record.type == "wallet recharge":

                # Update wallet
                wallet_status = WalletService.update_wallet(
                    transaction_type="Wallet Recharge",
                    credit=amount / 100,  # Convert to original currency
                    debit=0,
                    reference=payment_id,
                )

            elif payment_record.type == "shipping notifications":

                # Update wallet
                wallet_status = WalletService.update_notification_wallet(
                    transaction_type="notifications recharge",
                    credit=amount / 100,  # Convert to original currency
                    debit=0,
                    reference=payment_id,
                )

            if not wallet_status.status:
                logger.error(f"Wallet update failed for payment_id: {payment_id}")
                payment_record.status = "wallet update failed"
                db.add(payment_record)
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Wallet update failed.",
                )

            # Update payment record
            payment_record.status = "payment successful"
            payment_record.payment_id = payment_id
            payment_record.amount = amount / 100  # Store amount in original currency
            db.add(payment_record)
            db.commit()

            logger.info(f"Payment captured successfully for order_id: {order_id}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Payment captured successfully.",
            )
