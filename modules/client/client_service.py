import http
import uuid
from psycopg2 import DatabaseError
from sqlalchemy import func, cast, DateTime

from context_manager.context import context_user_data, get_db_session

# models
from models import (
    Client,
    Order,
)

# schema
from schema.base import GenericResponseModel
from .client_schema import (
    ClientInsertModel,
    ClientResponseModel,
    CompleteClientDetailsModel,
)
from modules.user.user_schema import UserInsertModel

# service
from modules.user.user_service import UserService

# utils
from database.utils import get_primary_key_by_uuid

from logger import logger


class ClientService:

    @staticmethod
    def create_client(
        client_data: ClientInsertModel,
    ) -> GenericResponseModel:
        try:

            company_id = context_user_data.get().company_id

            client = {
                "client_name": client_data.clientName,
                "company_id": company_id,
            }

            # convert the received object into an instance of the client model
            client_entity = Client(**client)

            # add user to database
            created_client = Client.create_client(client_entity)

            logger.info(
                msg="Client created successfully with uuid {}".format(
                    created_client.uuid
                ),
            )

            client_id = created_client.id

            userData = UserInsertModel(
                **{
                    "first_name": client_data.userFullName,
                    "last_name": "",
                    "email": client_data.userEmail,
                    "phone": client_data.userPhone,
                    "company_id": company_id,
                    "client_id": client_id,
                    "status": "active",
                    "password": client_data.password,
                }
            )

            UserService.create_user(user_data=userData)

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                data={"user_date": userData},
                message="Client created successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating client: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Client.",
            )

    @staticmethod
    def get_all_clients(clientFilters) -> GenericResponseModel:
        try:

            start_date = clientFilters.start_date
            end_date = clientFilters.end_date

            db = get_db_session()

            clients = db.query(Client).all()

            status_counts = (
                db.query(
                    Order.client_id, Order.status, func.count(Order.id).label("count")
                )
                .filter(
                    Order.status != "new",
                    Order.status != "cancelled",
                    cast(Order.booking_date, DateTime) >= start_date,
                    cast(Order.booking_date, DateTime) <= end_date,
                )
                .group_by(Order.client_id, Order.status)
                .all()
            )

            overall_status_counts = {"total": 0}

            order_status_map = {}
            for client_id, status, count in status_counts:
                # Initialize client-specific map
                if client_id not in order_status_map:
                    order_status_map[client_id] = {"total": 0}

                # Update overall status counts
                overall_status_counts["total"] += count
                overall_status_counts[status] = (
                    overall_status_counts.get(status, 0) + count
                )

                # Update client-specific counts
                if status not in ["new", "cancelled"]:
                    order_status_map[client_id]["total"] += count
                order_status_map[client_id][status] = count

            # Add overall status counts to the response
            response_data = []
            for client in clients:
                client_data = client.to_model().model_dump()  # Serialize client data
                client_id = client.id

                # Add status-wise order counts and total to the client's data
                client_data["order_status_counts"] = order_status_map.get(client_id, {})

                response_data.append(ClientResponseModel(**client_data))

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                data={
                    "clients": response_data,
                    "overall_status_counts": overall_status_counts,
                },
                message="Client created successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating client: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Client.",
            )
