import http
from decimal import Decimal
import os
from datetime import datetime
from pydantic import BaseModel
import razorpay
from psycopg2 import DatabaseError
import razorpay.errors
import hashlib
import requests
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Form, Request
import uuid

from context_manager.context import context_user_data, get_db_session
from logger import logger

# models
from models import Wallet, PaymentRecords

# schema
from schema.base import GenericResponseModel
from .payu_schema import PaymentRequest, PaymentResponse

# service
from modules.wallet.wallet_service import WalletService


class TempModel(BaseModel):
    client_id: int


class PayU:

    MERCHANT_KEY = os.getenv("PAYU_MERCHANT_KEY")
    MERCHANT_SALT = os.getenv("PAYU_SALT_256_BIT")

    # URLs
    PAYU_BASE_URL = (
        "https://secure.payu.in"  # Use https://secure.payu.in for production
    )
    SUCCESS_URL = "https://api.lastmiles.co/api/v1/payu/payments/success"
    FAILURE_URL = "https://api.lastmiles.co/api/v1/payu/payments/failure"
    CANCEL_URL = "https://api.lastmiles.co/api/v1/payu/payments/failure"

    @classmethod
    def get_payment_url(cls):
        return f"{cls.PAYU_BASE_URL}/_payment"

    def generate_hash(data: Dict[str, Any]) -> str:
        """Generate hash for PayU payment"""
        hash_string = f"{PayU.MERCHANT_KEY}|{data['txnid']}|{data['amount']}|{data['productinfo']}|{data['firstname']}|{data['email']}|||||||||||{PayU.MERCHANT_SALT}"
        return hashlib.sha512(hash_string.encode()).hexdigest()

    def verify_hash(data: Dict[str, Any]) -> bool:
        """Verify hash for payment response"""
        received_hash = data.get("hash")
        hash_string = f"{PayU.MERCHANT_SALT}|{data.get('status')}|||||||||||{data.get('email')}|{data.get('firstname')}|{data.get('productinfo')}|{data.get('amount')}|{data.get('txnid')}|{PayU.MERCHANT_KEY}"
        calculated_hash = hashlib.sha512(hash_string.encode()).hexdigest()
        return received_hash == calculated_hash

    def create_payment_params(order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create payment parameters for PayU"""
        payment_data = {
            "key": PayU.MERCHANT_KEY,
            "txnid": order_data["transaction_id"],
            "amount": str(order_data["amount"]),
            "productinfo": order_data["product_info"],
            "firstname": order_data["firstname"],
            "email": order_data["email"],
            "phone": order_data.get("phone", ""),
            "surl": PayU.SUCCESS_URL,
            "furl": PayU.FAILURE_URL,
            "curl": PayU.CANCEL_URL,
            "service_provider": "payu_paisa",
        }

        print(4)

        # Generate hash
        payment_data["hash"] = PayU.generate_hash(payment_data)

        print(5)

        return payment_data

    def create_payment(payment_request: PaymentRequest):
        try:

            client_id = context_user_data.get().client_id

            print(2)
            # Generate unique transaction ID
            transaction_id = str(uuid.uuid4())

            # Prepare order data
            order_data = {
                "transaction_id": transaction_id,
                "amount": payment_request.amount,
                "firstname": payment_request.firstname,
                "email": payment_request.email,
                "phone": payment_request.phone,
                "product_info": payment_request.product_info,
            }

            print(3)

            # Create payment parameters
            payment_params = PayU.create_payment_params(order_data)

            with get_db_session() as db:
                db.add(
                    PaymentRecords(
                        gateway="payu",
                        order_id=transaction_id,
                        status="payment initiated",
                        amount=payment_request.amount,
                        currency="INR",
                        type="wallet recharge",
                        client_id=client_id,
                    )
                )
                db.commit()

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Payment initiated successfully",
                data=PaymentResponse(
                    payment_url=PayU.get_payment_url(), payment_params=payment_params
                ),
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @staticmethod
    def payment_status_webhook_success(payment_req):
        try:
            # Extract event type and payload

            status = payment_req.get("status")

            if status != "Success":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Payment status is not successful",
                )

            print(1)

            payload = payment_req

            return PayU._handle_payment_success(payload)

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
    def payment_status_webhook_failed(payment_req):
        try:
            # Extract event type and payload

            print(1)

            payload = payment_req

            return PayU._handle_payment_failed(payload)

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
    def _handle_payment_failed(payload):
        with get_db_session() as db:
            order_id = payload.get("merchantTransactionId")
            payment_id = payload.get("paymentId")
            failure_reason = payload.get("error_Message", "Unknown error")

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
    def _handle_payment_success(payload):
        print(2)
        with get_db_session() as db:
            order_id = payload.get("merchantTransactionId")
            payment_id = payload.get("paymentId")
            amount = payload.get("amount")

            print(3)
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
            print(4)
            if not payment_record:
                logger.warning(f"No payment record found for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Payment record not found",
                )
            print(5)

            # Avoid duplicate updates
            if payment_record.status == "payment successful":
                logger.info(f"Duplicate captured event for order_id: {order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Duplicate webhook ignored",
                )
            print(6)

            client_id = payment_record.client_id
            context_user_data.set(TempModel(**{"client_id": client_id}))

            if payment_record.type == "wallet recharge":

                print(7)

                # Update wallet
                wallet_status = WalletService.update_wallet(
                    transaction_type="Wallet Recharge",
                    credit=float(amount),  # Convert to original currency
                    debit=0,
                    reference=payment_id,
                )

                print(8)

            elif payment_record.type == "shipping notifications":

                # Update wallet
                wallet_status = WalletService.update_notification_wallet(
                    transaction_type="notifications recharge",
                    credit=float(amount),  # Convert to original currency
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

            print(9)

            # Update payment record
            payment_record.status = "payment successful"
            payment_record.payment_id = payment_id
            payment_record.amount = amount  # Store amount in original currency
            payment_record.method = payload.get("paymentMode", "")
            db.add(payment_record)
            db.commit()

            print(10)

            logger.info(f"Payment captured successfully for order_id: {order_id}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Payment captured successfully.",
            )
