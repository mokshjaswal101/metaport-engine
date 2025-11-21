import http
from psycopg2 import DatabaseError
from sqlalchemy import asc
import re
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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
    async def create_pickup_location(
        pickup_location_data: PickupLocationInsertModel,
    ) -> GenericResponseModel:
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id

            # Sanitize numeric → string
            sanitized_data = pickup_location_data.model_dump()

            if sanitized_data.get("contact_person_phone"):
                sanitized_data["contact_person_phone"] = str(
                    sanitized_data["contact_person_phone"]
                )

            if sanitized_data.get("alternate_phone"):
                sanitized_data["alternate_phone"] = str(
                    sanitized_data["alternate_phone"]
                )

            if sanitized_data.get("pincode"):
                sanitized_data["pincode"] = str(sanitized_data["pincode"])

            async with get_db_session() as db:

                # ------------------------------
                # Generate unique location code
                # ------------------------------
                location_code = await Pickup_Location.generate_location_code(db)

                location_data = {
                    **sanitized_data,
                    "client_id": client_id,
                    "company_id": company_id,
                    "location_code": location_code,
                    "courier_location_codes": {},
                }

                # ------------------------------
                # Check if location name exists
                # ------------------------------
                stmt = select(Pickup_Location).where(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_name == sanitized_data["location_name"],
                )
                result = await db.execute(stmt)
                existing_location = result.scalar_one_or_none()

                if existing_location:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        message="Location name already exists",
                        data={"location_code": existing_location.location_code},
                    )

                # ------------------------------
                # Create Location
                # ------------------------------
                location_model_instance = Pickup_Location.create_db_entity(
                    location_data
                )

                db.add(location_model_instance)
                await db.commit()
                await db.refresh(location_model_instance)

            # Return response after session closes
            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Location created successfully",
                data={"location_code": location_model_instance.location_code},
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error creating Pickup Location: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Location.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    # @staticmethod
    # def set_default_location(
    #     pickup_location_id: str,
    # ) -> GenericResponseModel:
    #     try:

    #         user_data = context_user_data.get()
    #         client_id = user_data.client_id

    #         db = get_db_session()

    #         # find existing location id
    #         location = (
    #             db.query(Pickup_Location)
    #             .filter(
    #                 Pickup_Location.client_id == client_id,
    #                 Pickup_Location.location_code == pickup_location_id,
    #             )
    #             .first()
    #         )

    #         # throw error if not client location is not found
    #         if location is None:
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.CONFLICT,
    #                 message="Invalid location id",
    #             )

    #         # find current default
    #         current_default_location = (
    #             db.query(Pickup_Location)
    #             .filter(
    #                 Pickup_Location.client_id == client_id,
    #                 Pickup_Location.is_default == True,
    #             )
    #             .first()
    #         )

    #         # if exists → remove default flag
    #         if current_default_location:
    #             current_default_location.is_default = False
    #             db.add(current_default_location)

    #         # set new location as default
    #         location.is_default = True
    #         db.add(location)

    #         db.flush()
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.CREATED,
    #             status=True,
    #             message="Default Location updated successfully",
    #         )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Could not update default location: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Could not update default location",
    #         )

    #     except Exception as e:
    #         # Log other unhandled exceptions
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Unhandled error: {}".format(str(e)),
    #         )
    #         # Return a general internal server error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An internal server error occurred. Please try again later.",
    #         )

    @staticmethod
    async def set_default_location(pickup_location_id: str) -> GenericResponseModel:
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            async with get_db_session() as db:
                # Find the location to set as default
                stmt_location = select(Pickup_Location).where(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                )
                result_location = await db.execute(stmt_location)
                location = result_location.scalar_one_or_none()

                if not location:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        message="Invalid location id",
                        status=False,
                    )

                # Find current default location
                stmt_default = select(Pickup_Location).where(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_default == True,
                )
                result_default = await db.execute(stmt_default)
                current_default_location = result_default.scalar_one_or_none()

                # Remove default flag if exists
                if current_default_location:
                    current_default_location.is_default = False
                    db.add(current_default_location)

                # Set new location as default
                location.is_default = True
                db.add(location)

                await db.commit()
                await db.refresh(location)

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Default location updated successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error while updating default location: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update default location",
                status=False,
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error while updating default location: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
                status=False,
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

    # @staticmethod
    # def delete_location(
    #     pickup_location_id: str,
    # ) -> GenericResponseModel:
    #     try:

    #         user_data = context_user_data.get()
    #         client_id = user_data.client_id

    #         db = get_db_session()

    #         # find existing location id
    #         location = (
    #             db.query(Pickup_Location)
    #             .filter(
    #                 Pickup_Location.client_id == client_id,
    #                 Pickup_Location.location_code == pickup_location_id,
    #             )
    #             .first()
    #         )

    #         # throw error if not client location is not found
    #         if location is None:
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.CONFLICT,
    #                 message="Invalid location id",
    #             )

    #         location.is_deleted = True

    #         db.add(location)

    #         db.flush()

    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.CREATED,
    #             status=True,
    #             message="Location deleted successfully",
    #         )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Could not delete location: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Could not delete location",
    #         )

    #     except Exception as e:
    #         # Log other unhandled exceptions
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Unhandled error: {}".format(str(e)),
    #         )
    #         # Return a general internal server error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An internal server error occurred. Please try again later.",
    #         )
    @staticmethod
    async def delete_location(pickup_location_id: str) -> GenericResponseModel:
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id

            async with get_db_session() as db:
                # Find the location
                stmt_location = select(Pickup_Location).where(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                )
                result_location = await db.execute(stmt_location)
                location = result_location.scalar_one_or_none()

                if not location:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        message="Invalid location id",
                        status=False,
                    )

                # Soft delete
                location.is_deleted = True
                db.add(location)

                await db.commit()
                await db.refresh(location)

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Location deleted successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error while deleting location: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not delete location",
                status=False,
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error while deleting location: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
                status=False,
            )

    @staticmethod
    async def get_pickup_locations() -> GenericResponseModel:
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id

            db: AsyncSession = get_db_session()

            # Build async select query
            query = (
                select(Pickup_Location)
                .where(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.company_id == company_id,
                    Pickup_Location.is_deleted == False,
                )
                .order_by(asc(Pickup_Location.created_at))
            )

            result = await db.execute(query)
            locations = result.scalars().all()

            if locations:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=[
                        PickupLocationResponseModel(**location.to_model().model_dump())
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
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching Pickup Locations: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching pickup locations.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

        finally:
            # safely close async session
            if db:
                await db.close()
