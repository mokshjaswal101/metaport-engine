import http
from sqlalchemy import or_, desc, cast, func
from psycopg2 import DatabaseError
from typing import List, Any

from context_manager.context import context_user_data, get_db_session

from logger import logger

# schema
from schema.base import GenericResponseModel
from modules.ndr_history.ndr_history_schema import Ndr_History_Model

# models
from models import Ndr_history
from modules.ndr_history.ndr_history_schema import Ndr_History_Model

# from modules import


class NdrHistoryService:

    @staticmethod
    def Common_Insert_Query(db, order_id, ndr_id, ndr_list):
        ndr_history_list = [
            {
                "order_id": int(order_id),
                "ndr_id": int(ndr_id),
                "status": record["status"],
                "datetime": record["datetime"],
                "reason": record["description"],
            }
            for index, record in enumerate(ndr_list)
        ]
        db.bulk_insert_mappings(Ndr_history, ndr_history_list)
        db.commit()

    @staticmethod
    def create_ndr_history(
        ndr_list: Any,
        order_id: str,
        ndr_id: int,
    ):
        try:
            db = get_db_session()

            ndr_history_record = (
                db.query(Ndr_history)
                .filter(Ndr_history.order_id == order_id, Ndr_history.ndr_id == ndr_id)
                .all()
            )
            if len(ndr_history_record) == 0:

                # Insert a new one
                NdrHistoryService.Common_Insert_Query(db, order_id, ndr_id, ndr_list)

                logger.info("BULK HISTORY SAVE SUCCESSFULLY")

            else:
                # Check if the old history length is less than the new history length
                if len(ndr_list) > len(ndr_history_record):

                    # Delete old matching record
                    db.query(Ndr_history).filter(
                        Ndr_history.order_id == order_id,
                        Ndr_history.ndr_id == ndr_id,
                    ).delete()

                    db.commit()

                    # After deleting the old records, insert a new one
                    NdrHistoryService.Common_Insert_Query(
                        db, order_id, ndr_id, ndr_list
                    )

                    logger.info(
                        "BULK HISTORY SAVE SUCCESSFULLY IF OLD HISTORY NOT UPDATED"
                    )

                else:
                    print("error")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Ndr updated Successfully",
                    status=True,
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Order.",
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
