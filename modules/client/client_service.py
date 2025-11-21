import http
import uuid
from psycopg2 import DatabaseError
from sqlalchemy.future import select
from sqlalchemy import func, cast, DateTime

from context_manager.context import context_user_data, get_db_session

# models
from models import (
    Client,
    Order,
    Client_Onboarding_Details,
    # Client_Bank_Details,
)

# schema
from schema.base import GenericResponseModel
from .client_schema import (
    ClientInsertModel,
    ClientResponseModel,
    CompleteClientDetailsModel,
    ClientOnboardingDetailsModel,
    ClientBankDetailsModel,
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

    @staticmethod
    async def get_complete_client_details() -> GenericResponseModel:
        """
        Get complete client details including onboarding details and bank details asynchronously.
        """
        try:
            client_id = context_user_data.get().client_id

            async with get_db_session() as db:  # This should be an AsyncSession
                # Fetch client data
                result_client = await db.execute(
                    select(Client).filter(Client.id == client_id)
                )
                client = result_client.scalars().first()
                if not client:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Client not found",
                    )

                # Fetch onboarding details
                result_onboarding = await db.execute(
                    select(Client_Onboarding_Details).filter(
                        Client_Onboarding_Details.client_id == client_id
                    )
                )
                onboarding_details = result_onboarding.scalars().first()

                # Prepare onboarding model
                onboarding_model = None
                if onboarding_details:
                    onboarding_model = ClientOnboardingDetailsModel(
                        id=onboarding_details.id,
                        onboarding_user_id=onboarding_details.onboarding_user_id,
                        client_id=onboarding_details.client_id,
                        company_legal_name=onboarding_details.company_legal_name,
                        company_name=onboarding_details.company_name,
                        office_address=onboarding_details.office_address,
                        landmark=onboarding_details.landmark,
                        pincode=onboarding_details.pincode,
                        city=onboarding_details.city,
                        state=onboarding_details.state,
                        country=onboarding_details.country,
                        phone_number=onboarding_details.phone_number,
                        email=onboarding_details.email,
                        pan_number=onboarding_details.pan_number,
                        upload_pan=onboarding_details.upload_pan,
                        is_coi=onboarding_details.is_coi,
                        coi=onboarding_details.coi,
                        upload_coi=onboarding_details.upload_coi,
                        is_gst=onboarding_details.is_gst,
                        gst=onboarding_details.gst,
                        upload_gst=onboarding_details.upload_gst,
                        aadhar_card=onboarding_details.aadhar_card,
                        upload_aadhar_card_front=onboarding_details.upload_aadhar_card_front,
                        upload_aadhar_card_back=onboarding_details.upload_aadhar_card_back,
                        is_cod_order=onboarding_details.is_cod_order,
                        is_stepper=onboarding_details.is_stepper,
                        is_company_details=onboarding_details.is_company_details,
                        is_billing_details=onboarding_details.is_billing_details,
                        is_term=onboarding_details.is_term,
                        is_review=onboarding_details.is_review,
                        is_form_access=onboarding_details.is_form_access,
                    )

                # Prepare client model
                client_model = client.to_model()

                complete_details = CompleteClientDetailsModel(
                    client_data=client_model,
                    onboarding_details=onboarding_model,
                    # bank_details=None  # Add bank details if needed
                )

                logger.info(
                    f"Complete client details retrieved successfully for client_id {client_id}"
                )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=complete_details.dict(),
                    message="Complete client details retrieved successfully",
                )

        except DatabaseError as e:
            logger.error(
                f"Database error retrieving complete client details: {str(e)}",
                extra=context_user_data.get(),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while retrieving complete client details.",
            )

        except Exception as e:
            logger.error(
                f"Unhandled error retrieving complete client details: {str(e)}",
                extra=context_user_data.get(),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred.",
            )
