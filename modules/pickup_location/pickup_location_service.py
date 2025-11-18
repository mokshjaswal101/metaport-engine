import http
from psycopg2 import DatabaseError
from sqlalchemy import asc
import re
from fastapi.encoders import jsonable_encoder

from context_manager.context import context_user_data, get_db_session

from logger import logger

# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from .pickup_location_schema import (
    PickupLocationInsertModel,
    PickupLocationResponseModel,
)


class PickupLocationService:

    @staticmethod
    def create_pickup_location(
        pickup_location_data: PickupLocationInsertModel,
    ) -> GenericResponseModel:
        try:

            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id

            location_data = {
                **pickup_location_data.model_dump(),  # Convert pydantic model to dict
                "client_id": client_id,
                "company_id": company_id,
                "location_code": Pickup_Location.generate_location_code(),
                "courier_location_codes": {},
            }

            with get_db_session() as db:

                # check existing location name
                location = (
                    db.query(Pickup_Location)
                    .filter(
                        Pickup_Location.client_id == client_id,
                        Pickup_Location.location_name
                        == pickup_location_data.location_name,
                    )
                    .first()
                )

                if location:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        message="Location name already exists",
                        data={"location_code": location.location_code},
                    )

            location_model_instance = Pickup_Location.create_db_entity(location_data)
            created_location = Pickup_Location.create_new_location(
                location_model_instance
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Location created successfully",
                data={"location_code": created_location.location_code},
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Pickup Location: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Location.",
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
    def set_default_location(
        pickup_location_id: str,
    ) -> GenericResponseModel:
        try:

            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # find existing location id
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                )
                .first()
            )

            # throw error if not client location is not found
            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message="Invalid location id",
                )

            # find current default
            current_default_location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_default == True,
                )
                .first()
            )

            # if exists → remove default flag
            if current_default_location:
                current_default_location.is_default = False
                db.add(current_default_location)

            # set new location as default
            location.is_default = True
            db.add(location)

            db.flush()
            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Default Location updated successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Could not update default location: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update default location",
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
    def change_location_status(
        pickup_location_id: str,
    ) -> GenericResponseModel:
        try:

            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # find existing location id
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                )
                .first()
            )

            # throw error if not client location is not found
            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message="Invalid location id",
                )

            current_default_location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_default == True,
                )
                .first()
            )

            # remove the default parameter from current location and set it to the new one

            current_default_location.is_default = False
            location.is_default = True

            db.add(current_default_location)
            db.add(location)

            db.flush()

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Default Location updated successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Could not update default location: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update default location",
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
    def delete_location(
        pickup_location_id: str,
    ) -> GenericResponseModel:
        try:

            user_data = context_user_data.get()
            client_id = user_data.client_id

            db = get_db_session()

            # find existing location id
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                )
                .first()
            )

            # throw error if not client location is not found
            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message="Invalid location id",
                )

            location.is_deleted = True

            db.add(location)

            db.flush()

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Location deleted successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Could not delete location: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not delete location",
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
    def get_pickup_locations() -> GenericResponseModel:
        try:

            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id

            with get_db_session() as db:
                # Query the database for pickup locations matching the client_id and company_id
                locations = (
                    db.query(Pickup_Location)
                    .filter(
                        Pickup_Location.client_id == client_id,
                        Pickup_Location.company_id == company_id,
                        Pickup_Location.is_deleted == False,
                    )
                    .order_by(asc(Pickup_Location.created_at))
                    .all()
                )

            # Check if any locations were found
            if locations:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=[
                        PickupLocationResponseModel(
                            **location.to_model().model_dump(),
                        )
                        for location in locations
                    ],
                    message="Pickup locations fetched successfully",
                )
            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    status=False,
                    message="No pickup locations found.",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating Pickup Location: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Location.",
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
