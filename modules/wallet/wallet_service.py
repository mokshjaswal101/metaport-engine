import http
from decimal import Decimal
from sqlalchemy import desc
from datetime import datetime
from pytz import timezone
import os
from psycopg2 import DatabaseError
from sqlalchemy.types import DateTime, String
from sqlalchemy.orm import joinedload
from sqlalchemy import (
    cast,
)

from context_manager.context import context_user_data, get_db_session
from logger import logger

# models
from models import Wallet, Wallet_Logs, PaymentRecords

# schema
from schema.base import GenericResponseModel
from .wallet_schema import WalletResponseModel, log_filters, rechargeRecordFilters

# service
from modules.wallet_logs.wallet_logs_service import WalletLogsService


UTC = timezone("UTC")


class WalletService:

    @staticmethod
    def get_balance():

        try:
            print(f"Number of CPUs: {os.cpu_count()}")

            client_id = context_user_data.get().client_id

            db = get_db_session()
            print(client_id, "hello acbd =>>")

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()
            print(wallet, "Find action")
            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Wallet not found",
                )
            print("Wallet Found")
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=WalletResponseModel(**wallet.to_model().model_dump()),
                message="successfull",
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
    def update_wallet(reference, credit=0, debit=0, transaction_type="Freight"):

        try:
            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.OK,
                    message="Wallet not found",
                )

            current_balance = wallet.amount

            # deduct amount from wallet in case of debit
            if debit > 0:
                current_balance -= Decimal(debit)

            if credit > 0:
                current_balance += Decimal(credit)

            # update the wallet balance
            wallet.amount = current_balance

            cod_amount = wallet.cod_amount

            db.add(wallet)
            db.flush()
            db.commit()

            # create a log for the udpate
            WalletLogsService.add_new_log(
                transaction_type=transaction_type,
                credit=credit,
                debit=debit,
                wallet_balance_amount=current_balance,
                cod_balance_amount=cod_amount,
                reference=reference,
            )

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Unable to update wallet: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unable to update wallet",
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
                message="Unable to update wallet",
            )

    @staticmethod
    def update_notification_wallet(
        reference, credit=0, debit=0, transaction_type="notifications recharge"
    ):

        try:
            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.OK,
                    message="Wallet not found",
                )

            current_balance = wallet.shipping_notifications

            # deduct amount from wallet in case of debit
            if debit > 0:
                current_balance -= Decimal(debit)

            if credit > 0:
                current_balance += Decimal(credit)

            # update the wallet balance
            wallet.shipping_notifications = current_balance

            db.add(wallet)
            db.flush()
            db.commit()

            # create a log for the udpate
            WalletLogsService.add_new_log(
                transaction_type=transaction_type,
                credit=credit,
                debit=debit,
                wallet_balance_amount=wallet.amount,
                cod_balance_amount=wallet.cod_amount,
                reference=reference,
            )

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Unable to update wallet: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unable to update wallet",
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
                message="Unable to update wallet",
            )

    @staticmethod
    def check_sufficient_balance(amount: float):

        try:

            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            # if no wallet is found, return error message
            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.OK,
                    message="Wallet not found",
                )

            # for prepaid wallet

            if wallet.wallet_type == "prepaid":

                if amount > wallet.amount:
                    return GenericResponseModel(
                        status=False,
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Insufficient Balance",
                    )

                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="Sufficient Balance",
                )

            # for COD - wallet

            if wallet.wallet_type == "COD":

                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Not allowed kindly change the wallet type",
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
    def deduct_money(
        amount, awb_number="", transaction_type="Freight", cod_charge=0, date=None
    ):

        try:

            if date is None:
                date = datetime.now(UTC)

            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="Wallet not found",
                )

            # for prepaid orders, deduct directly from wallet amount
            if wallet.wallet_type == "prepaid":

                wallet.amount = wallet.amount - Decimal(amount)

            if wallet.wallet_type == "COD":

                wallet.amount = wallet.amount - Decimal(amount)

            log = {
                "datetime": date if date else datetime.now(UTC),
                "transaction_type": transaction_type,
                "credit": cod_charge,
                "debit": amount,
                "wallet_balance_amount": wallet.amount,
                "cod_balance_amount": wallet.cod_amount,
                "reference": "awb - " + awb_number,
                "client_id": client_id,
                "wallet_id": wallet.id,
            }

            log = Wallet_Logs(**log)

            db.add(log)
            db.flush()

            db.add(wallet)

            db.flush()

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Successfull",
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
    def add_money(amount):

        try:

            client_id = context_user_data.get().client_id

            with get_db_session() as db:

                wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

                if wallet is None:
                    # Return error response
                    return GenericResponseModel(
                        status=True,
                        status_code=http.HTTPStatus.OK,
                        message="Wallet not found",
                    )

                wallet.amount = wallet.amount + amount

                db.add(wallet)
                db.commit()

                print("yippeeee")

                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="Successfull",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error adding funds: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in adding funds",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Error in adding funds {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in adding funds",
            )

    @staticmethod
    def add_provisional_cod(amount):

        try:

            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="Wallet not found",
                )

            wallet.provisional_cod_amount = wallet.provisional_cod_amount + Decimal(
                amount
            )

            db.add(wallet)
            db.flush()

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error adding funds: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in adding funds",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Error in adding funds {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in adding funds",
            )

    @staticmethod
    def add_log(
        datetime,
        transaction_type,
        credit,
        debit,
        wallet_balance_amount,
        cod_balance_amount,
        reference="",
        description="",
    ):

        try:

            client_id = context_user_data.get().client_id

            with get_db_session() as db:

                wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

                # creating a log for this
                log = {
                    "datetime": datetime,
                    "transaction_type": transaction_type,
                    "credit": credit,
                    "debit": debit,
                    "wallet_balance_amount": wallet_balance_amount,
                    "cod_balance_amount": cod_balance_amount,
                    "reference": reference,
                    "description": description,
                    "client_id": client_id,
                    "wallet_id": wallet.id,
                }

                log = Wallet_Logs(**log)

                db.add(log)
                db.commit()

                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="Successfull",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error adding funds: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in adding funds",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Error in adding funds {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in adding funds",
            )

    @staticmethod
    def get_wallet_logs(filters: log_filters):

        try:

            page_number = filters.page_number
            batch_size = filters.batch_size
            log_type = filters.log_type
            start_date = filters.start_date
            end_date = filters.end_date

            client_id = context_user_data.get().client_id

            with get_db_session() as db:

                query = db.query(Wallet_Logs).filter(
                    Wallet_Logs.client_id == client_id,
                    Wallet_Logs.transaction_type != "notifications recharge",
                )

                query = query.filter(
                    cast(Wallet_Logs.datetime, DateTime) >= start_date,
                    cast(Wallet_Logs.datetime, DateTime) <= end_date,
                )

                if log_type:
                    query = query.filter(Wallet_Logs.transaction_type == log_type)

                total_count = query.count()

                query = query.order_by(desc(Wallet_Logs.datetime))

                offset_value = (page_number - 1) * batch_size
                query = query.offset(offset_value).limit(batch_size)

                wallet_logs = query.all()

                logs = [log.to_model().model_dump() for log in wallet_logs]

                return GenericResponseModel(
                    status=True,
                    data={"logs": logs, "total_count": total_count},
                    status_code=http.HTTPStatus.OK,
                    message="Successfull",
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
                message="Unable to get Logs",
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
    def get_recharge_records(filters: rechargeRecordFilters):

        try:

            from modules.razorpay.razorpay_schema import PaymentRecordModel

            page_number = filters.page_number
            batch_size = filters.batch_size
            start_date = filters.start_date
            end_date = filters.end_date

            with get_db_session() as db:

                query = db.query(PaymentRecords)

                if start_date and end_date:
                    query = query.filter(
                        cast(PaymentRecords.updated_at, DateTime) >= start_date,
                        cast(PaymentRecords.updated_at, DateTime) <= end_date,
                    )

                total_count = query.count()

                query = query.order_by(desc(PaymentRecords.updated_at))

                offset_value = (page_number - 1) * batch_size
                query = query.offset(offset_value).limit(batch_size)

                payment_records = query.options(joinedload(PaymentRecords.client)).all()

                records = [
                    PaymentRecordModel(**record.to_model().model_dump())
                    for record in payment_records
                ]

                print("heello")

                return GenericResponseModel(
                    status=True,
                    data={"records": records, "total_count": total_count},
                    status_code=http.HTTPStatus.OK,
                    message="Successfull",
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
                message="Unable to get Logs",
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
