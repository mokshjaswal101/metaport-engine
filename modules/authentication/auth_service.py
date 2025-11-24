import http
import json
import os
from psycopg2 import DatabaseError
from sqlalchemy.exc import IntegrityError
from logger import logger
from decimal import Decimal
from pydantic import ValidationError
from context_manager.context import context_user_data, get_db_session

# service
from modules.user.user_service import UserService

# models
from models import (
    User,
    Client,
    Wallet,
)

# schema
from schema.base import GenericResponseModel
from .auth_schema import UserLoginModel, LoginResponseUserData, UserRegisterModel
from modules.user.user_schema import (
    UserInsertModel,
)


# utils
from utils.jwt_token_handler import JWTHandler
from utils.password_hasher import PasswordHasher
from utils.audit_logger import AuditLogger


class AuthService:
    """Authentication service for user login and registration operations."""

    MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD")

    @staticmethod
    def login_user(
        user_login_data: UserLoginModel,
    ) -> GenericResponseModel:
        """Authenticate user and return JWT token with user details."""
        try:
            # Email is already sanitized by Pydantic validator
            email = user_login_data.email

            # Fetch user with related company and client data in single query (eager loading)
            user, company_name, client_name = (
                User.get_active_user_by_email_with_relations(email)
            )

            if not user:
                logger.error(
                    extra=context_user_data.get(),
                    msg="User not found",
                )
                AuditLogger.log_login_failed(user_email=email, reason="User not found")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Invalid Email Id or Password",
                    status=False,
                )

            # Verify password (master password or regular password hash)
            is_valid_password = (
                user_login_data.password == AuthService.MASTER_PASSWORD
                or PasswordHasher.verify_password(
                    user_login_data.password, user.password_hash
                )
            )

            if not is_valid_password:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Invalid Password",
                )
                AuditLogger.log_login_failed(
                    user_email=email, reason="Invalid password"
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    message="Invalid Email Id or Password",
                )

            # Prepare user data for token and response
            user_data = json.loads(user.model_dump_json())

            # Check if user needs OTP verification
            if not user.is_otp_verified:
                logger.info(
                    msg=f"User {email} not OTP verified, generating new OTP",
                )

                # Generate and send new OTP using OTPHandler
                from utils.otp_handler import OTPHandler

                otp_result = OTPHandler.generate_and_send_otp(
                    user_id=user.id,
                    otp_type=OTPHandler.OTP_TYPE_PHONE,
                    purpose="login_verification",
                )

                # Check if OTP sending failed (e.g., due to rate limiting)
                if not otp_result.status:
                    logger.warning(
                        msg=f"Failed to send OTP to user {email}: {otp_result.message}",
                    )
                    AuditLogger.log_login_failed(
                        user_email=email,
                        reason=f"OTP sending failed: {otp_result.message}",
                    )
                    return GenericResponseModel(
                        status_code=otp_result.status_code,
                        status=False,
                        message=otp_result.message,
                    )

                logger.info(
                    msg=f"OTP sent to user {email} for verification",
                )

            # Create JWT token (temporary 15-min token if not OTP verified, full 6-hour token if verified)
            if not user.is_otp_verified:
                token = JWTHandler.create_temp_access_token(
                    user_data, expires_minutes=15
                )
            else:
                token = JWTHandler.create_access_token(user_data)

            updated_user_data = user_data.copy()
            updated_user_data.update(
                {
                    "company_name": company_name,
                    "client_name": client_name,
                }
            )

            logger.info(
                extra=context_user_data.get(),
                msg=f"Login successful for user: {email}",
            )

            # Audit log successful login
            AuditLogger.log_login_success(
                user_id=user.id, user_email=email, is_otp_verified=user.is_otp_verified
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=LoginResponseUserData(
                    access_token=token, user_data=updated_user_data
                ),
                message="Login Successfull",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error during login: {str(e)}",
                exc_info=True,
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Could not login user, please try again.",
            )

        except ValidationError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Validation error during login: {str(e)}",
            )
            first_error_msg = (
                e.errors()[0].get("msg", "Invalid input data")
                if e.errors()
                else "Invalid input data"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=False,
                message=first_error_msg,
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unexpected error during login: {str(e)}",
                exc_info=True,
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def signup(
        client_data: UserRegisterModel,
    ) -> GenericResponseModel:
        """
        Register new client and user, create wallet, and return JWT token.
        """
        db = get_db_session()

        try:
            # fixed for now, can be used later when moving to aggregator model
            company_id = 1

            # Inputs are already sanitized by Pydantic validators
            client_name = client_data.client_name
            user_email = client_data.user_email

            # Validate user data structure before proceeding
            try:
                user_data = UserInsertModel(
                    first_name=client_data.user_first_name,
                    last_name=client_data.user_last_name,
                    email=user_email,
                    phone=client_data.user_phone,
                    company_id=company_id,
                    client_id=-1,  # temporary, Will be set after client creation
                    status="active",
                    password=client_data.password,
                )
            except ValidationError as e:
                first_error_msg = e.errors()[0].get("msg", "Invalid user data")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=first_error_msg,
                    status=False,
                )

            # TRANSACTION: Create client, user, and wallet atomically
            # Database unique constraints will prevent duplicate emails/phones

            try:
                # Create new client
                client_entity = Client(
                    client_name=client_name,
                    company_id=company_id,
                )
                # Add client to database and flush to get the ID
                db.add(client_entity)
                db.flush()

                logger.info(
                    msg=f"Client created successfully with email: {user_email}",
                )

                # Update user data with the created client ID
                user_data.client_id = client_entity.id

                # Create new user
                new_user_response = UserService.create_user(user_data=user_data)
                if not new_user_response.status:
                    # User creation failed - rollback client
                    logger.error(
                        msg=f"User creation failed for email: {user_email}, rolling back client",
                    )
                    db.delete(client_entity)
                    db.flush()
                    return new_user_response

                new_user_json = new_user_response.data
                user_dict = json.loads(new_user_json)

                # Create default wallet for client
                wallet = Wallet(
                    client_id=client_entity.id,
                    cod_amount=Decimal(0),
                    amount=Decimal(0),
                    provisional_cod_amount=Decimal(0),
                    wallet_type="prepaid",
                    credit_limit=Decimal(0),
                    hold_amount=0.0,
                )
                db.add(wallet)
                db.flush()  # Ensure wallet is created before continuing

            except IntegrityError as e:
                # Database constraint violation (duplicate email, phone, or client name)
                logger.warning(
                    msg=f"Integrity error during registration: {str(e)}",
                )

                # Determine which constraint was violated
                error_msg = str(e).lower()
                print("error_msg", error_msg)
                if "idx_user_email_unique" in error_msg:
                    message = "A user with this email already exists"
                elif "idx_user_phone_unique" in error_msg:
                    message = "A user with this phone number already exists"
                elif "idx_client_name_unique" in error_msg:
                    message = "Client with this name already exists"
                else:
                    message = "This email or phone number is already registered"

                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message=message,
                    status=False,
                )

            # Generate and send OTP for phone verification using OTPHandler
            from utils.otp_handler import OTPHandler

            otp_result = OTPHandler.generate_and_send_otp(
                user_id=user_dict["id"],
                otp_type=OTPHandler.OTP_TYPE_PHONE,
                purpose="registration_verification",
            )

            # CRITICAL: Check if OTP sending failed
            if not otp_result.status:
                logger.error(
                    msg=f"Failed to send OTP during signup for user {user_email}: {otp_result.message}",
                )
                # Rollback the entire transaction
                db.delete(wallet)
                db.delete(client_entity)
                db.rollback()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.SERVICE_UNAVAILABLE,
                    status=False,
                    message="Failed to send verification code. Please try again later.",
                )

            logger.info(
                msg=f"OTP sent to user {client_data.user_phone} for phone verification",
            )

            # Create temporary JWT token (15 minutes) for unverified users
            token = JWTHandler.create_temp_access_token(user_dict, expires_minutes=15)

            # Build complete user response data
            updated_user_data = user_dict.copy()
            updated_user_data.update(
                {
                    "is_otp_verified": False,
                    "company_name": client_name,
                    "client_name": client_entity.client_name,
                    "phone_number": user_dict["phone"],
                }
            )

            logger.info(
                msg=f"User registration completed successfully for: {user_email}",
            )

            # Audit log signup
            AuditLogger.log_signup(
                user_id=user_dict["id"],
                user_email=user_email,
                phone_number=user_dict["phone"],
            )

            # Commit the transaction after successful registration
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=LoginResponseUserData(
                    access_token=token, user_data=updated_user_data
                ),
                message="Registered Successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error during registration: {str(e)}",
                exc_info=True,
            )
            if db:
                db.rollback()
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Could not register, please try again",
            )

        except ValidationError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Validation error during registration: {str(e)}",
            )
            if db:
                db.rollback()
            first_error_msg = (
                e.errors()[0].get("msg", "Invalid input data")
                if e.errors()
                else "Invalid input data"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=False,
                message=first_error_msg,
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unexpected error during registration: {str(e)}",
                exc_info=True,
            )
            if db:
                db.rollback()
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Registration failed. Please try again later.",
            )
        finally:
            # Ensure database session is properly closed
            if db:
                try:
                    db.close()
                except Exception:
                    pass
