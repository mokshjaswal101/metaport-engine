import http
from psycopg2 import DatabaseError
from sqlalchemy.future import select
import asyncio
import re

from context_manager.context import context_user_data, get_db_session

# models
from models import User

# schema
from schema.base import GenericResponseModel

from .user_schema import UserInsertModel, UserModel, ChangePasswordModel


# utils
from utils.password_hasher import PasswordHasher

from logger import logger


def password_validation(password):
    error = ""
    if len(password) < 8:
        error = "new password must be at least 8 characters long"
    if not re.search(r"[a-z]", password):
        error = "new password must contain at least one lowercase letter"
    if not re.search(r"[0-9]", password):
        error = "new password must contain at least one number"
    if not re.search(r"[\W_]", password):  # Special character (non-alphanumeric)
        error = "new password must contain at least one special character"
    return error


class UserService:

    # @staticmethod
    # def create_user(
    #     user_data: UserInsertModel,
    # ) -> GenericResponseModel:
    #     try:

    #         user: UserModel = User.get_active_user_by_email(user_data.email)

    #         if user:
    #             logger.error(
    #                 extra=context_user_data.get(),
    #                 msg="User with this email already exists",
    #             )
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.BAD_REQUEST,
    #                 message="User with this email already exists",
    #                 status=False,
    #             )

    #         company_id = user_data.company_id
    #         client_id = user_data.client_id

    #         # if no primary key is found, return error message for invalid id
    #         if company_id is None or client_id is None:
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.BAD_REQUEST,
    #                 message="Invalid company or client Id",
    #             )

    #         hashed_password = PasswordHasher.get_password_hash(user_data.password)

    #         user_entity = user_data.model_dump()

    #         # update the values
    #         user_entity["client_id"] = client_id
    #         user_entity["company_id"] = company_id
    #         user_entity["password_hash"] = hashed_password
    #         user_entity.pop("password")

    #         user_model_instance = User(**user_entity)

    #         # create the record in the db
    #         created_user = User.create_user(user_model_instance)

    #         logger.info(
    #             msg="User created successfully with uuid {}".format(created_user.uuid),
    #         )

    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.CREATED,
    #             status=True,
    #             message="User created successfully",
    #             data=created_user.model_dump_json(),
    #         )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Error creating company: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An error occurred while creating the Company.",
    #         )

    @staticmethod
    async def create_user(
        user_data: UserInsertModel,
    ) -> GenericResponseModel:
        try:
            # Await the async call
            user: UserModel = await User.get_active_user_by_email(user_data.email)

            if user:
                logger.error(
                    extra=context_user_data.get(),
                    msg="User with this email already exists",
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="User with this email already exists",
                    status=False,
                )

            company_id = user_data.company_id
            client_id = user_data.client_id

            if company_id is None or client_id is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid company or client Id",
                )

            hashed_password = PasswordHasher.get_password_hash(user_data.password)
            user_entity = user_data.model_dump()
            user_entity["client_id"] = client_id
            user_entity["company_id"] = company_id
            user_entity["password_hash"] = hashed_password
            user_entity.pop("password")

            user_model_instance = User(**user_entity)

            # Await async create_user call if User.create_user is async
            created_user = await User.create_user(user_model_instance)

            logger.info(
                msg="User created successfully with uuid {}".format(created_user.uuid),
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="User created successfully",
                data=created_user.model_dump_json(),
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating company: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Company.",
            )

    # @staticmethod
    # def change_password(
    #     user_data: ChangePasswordModel,
    # ) -> GenericResponseModel:
    #     try:

    #         with get_db_session() as db:

    #             old_password = user_data.old_password
    #             new_password = user_data.new_password

    #             email = context_user_data.get().email

    #             user = db.query(User).filter(User.email == email).first()

    #             if not user:

    #                 return GenericResponseModel(
    #                     status_code=http.HTTPStatus.BAD_REQUEST,
    #                     message="User Not Found.",
    #                 )

    #             if not PasswordHasher.verify_password(
    #                 user_data.old_password, user.password_hash
    #             ):

    #                 logger.error(
    #                     extra=context_user_data.get(),
    #                     msg="Incorrect Password",
    #                 )
    #                 return GenericResponseModel(
    #                     status_code=http.HTTPStatus.UNAUTHORIZED,
    #                     message="Incorrect Password",
    #                 )

    #             error = password_validation(user_data.new_password)
    #             if error:
    #                 return GenericResponseModel(
    #                     status_code=http.HTTPStatus.BAD_REQUEST,
    #                     message=error,
    #                     status=False,
    #                 )

    #             hashed_password = PasswordHasher.get_password_hash(
    #                 user_data.new_password
    #             )

    #             user.password_hash = hashed_password

    #             db.add(user)
    #             db.flush()

    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.OK,
    #             status=True,
    #             message="Password CHanged",
    #         )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Error: {}".format(str(e)),
    #         )
    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Could not login user, please try again.",
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
    async def change_password(
        user_data: ChangePasswordModel,
    ) -> GenericResponseModel:
        try:
            async with get_db_session() as db:  # AsyncSession required
                email = context_user_data.get().email

                result = await db.execute(select(User).filter(User.email == email))
                user = result.scalars().first()

                if not user:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="User not found",
                    )

                # Verify old password (blocking hash, run in thread)
                valid_old = await asyncio.to_thread(
                    PasswordHasher.verify_password,
                    user_data.old_password,
                    user.password_hash,
                )
                if not valid_old:
                    logger.error(
                        extra=context_user_data.get(), msg="Incorrect Password"
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.UNAUTHORIZED,
                        message="Incorrect password",
                    )

                # Validate new password
                error = await asyncio.to_thread(
                    password_validation, user_data.new_password
                )
                if error:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=error,
                        status=False,
                    )

                # Hash new password (blocking, run in thread)
                hashed_password = await asyncio.to_thread(
                    PasswordHasher.get_password_hash, user_data.new_password
                )
                user.password_hash = hashed_password
                db.add(user)
                await db.commit()  # Async commit

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Password changed successfully",
            )

        except DatabaseError as e:
            logger.error(extra=context_user_data.get(), msg=f"Database error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not change password due to database error",
            )
        except Exception as e:
            logger.error(
                extra=context_user_data.get(), msg=f"Unhandled error: {str(e)}"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later",
            )
