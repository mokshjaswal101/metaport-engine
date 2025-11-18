import http
from decimal import Decimal
from sqlalchemy import desc, asc

from datetime import datetime
import pytz


from pytz import timezone


from psycopg2 import DatabaseError

from context_manager.context import context_user_data, get_db_session

# models
from models import Wallet, Wallet_Logs, COD_Remittance

# schema
from schema.base import GenericResponseModel

# utils
from logger import logger

UTC = timezone("UTC")


class WalletLogsService:

    @staticmethod
    def add_new_log(
        transaction_type,
        wallet_balance_amount,
        cod_balance_amount,
        reference="",
        description="",
        log_datetime=None,
        credit=0,
        debit=0,
    ):

        try:

            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            # creating the log structure
            new_log = {
                "datetime": log_datetime if log_datetime else datetime.now(UTC),
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

            new_log = Wallet_Logs(**new_log)

            db.add(new_log)
            db.flush()
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
                msg="Error creating log: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not create a wallet log",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Error in creating log {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in creating log",
            )
