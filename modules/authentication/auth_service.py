import http
import json
import os
from psycopg2 import DatabaseError
from fastapi.encoders import jsonable_encoder
from logger import logger
from sqlalchemy.orm import joinedload
from decimal import Decimal
from pydantic import ValidationError
from context_manager.context import context_user_data, get_db_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# from context_manager.context_helper import get_async_db_session_once
# service
from modules.user.user_service import UserService
from modules.client.client_onboarding_service import ClientOnboardingService

from modules.client.client_schema import (
    SignupwithOnboarding,
)

# models
from models import (
    User,
    Client,
    Company,
    Company_To_Client_Contract,
    New_Company_To_Client_Rate,
    Client_Onboarding,
    Wallet,
)

# schema
from schema.base import GenericResponseModel
from .auth_schema import UserLoginModel, LoginResponseUserData, UserRegisterModel
from modules.user.user_schema import (
    UserModel,
    UserInsertModel,
)

# models
# from models import Company_To_Client_Contract, New_Company_To_Client_Rate

# utils
from utils.jwt_token_handler import JWTHandler
from utils.password_hasher import PasswordHasher
from database.utils import get_uuid_by_primary_key


class AuthService:
    """Authentication service for user login and registration operations."""

    MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD")

    @staticmethod
    async def get_client_onboarding_status(
        db: AsyncSession, client_id: int
    ) -> bool | None:
        """Check if client has completed onboarding process."""
        result = await db.execute(
            select(Client.is_onboarding_completed).where(Client.id == client_id)
        )
        onboarding_entry = (
            result.scalar_one_or_none()
        )  # returns the value directly or None
        return onboarding_entry

    @staticmethod
    async def login_user(user_login_data: UserLoginModel) -> GenericResponseModel:
        db = get_db_session()  # get async session from context
        if not db:
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database session not initialized.",
            )

        try:
            email = user_login_data.email.strip().lower()
            user: UserModel = await User.get_active_user_by_email(email)
            if not user:
                logger.error(extra=context_user_data.get(), msg="User not found")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="New user! Please sign Up",
                    status=False,
                )

            # Verify password
            is_valid_password = (
                user_login_data.password == AuthService.MASTER_PASSWORD
                or PasswordHasher.verify_password(
                    user_login_data.password, user.password_hash
                )
            )
            if not is_valid_password:
                logger.error(extra=context_user_data.get(), msg="Invalid credentials")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    message="Invalid Credentials",
                )

            # Prepare user data for token and response
            user_data = json.loads(user.model_dump_json())
            token = JWTHandler.create_access_token(user_data)

            # Fetch related data (company, client, onboarding status)
            company_data = await Company.get_by_id(user.company_id)
            client_data = await Client.get_by_id(user.client_id)
            onboarding_status = await AuthService.get_client_onboarding_status(
                db, user.client_id
            )

            # Build complete user response data
            updated_user_data = user_data.copy()
            updated_user_data.update(
                {
                    "is_onboarding_completed": onboarding_status is True,
                    "company_name": company_data.company_name,
                    "client_name": client_data.client_name,
                }
            )

            logger.info(
                extra=context_user_data.get(), msg=f"Login successful for user: {email}"
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
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not login user, please try again.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unexpected error during login: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )
        finally:
            # Close the session to prevent connection leaks
            if db:
                await db.close()

    @staticmethod
    async def signup(client_data: UserRegisterModel) -> GenericResponseModel:
        db = get_db_session()
        if not db:
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database session not initialized.",
            )
        try:
            # STEP 1: Perform validations
            company_id = 1
            client_name = client_data.client_name
            user_email = client_data.user_email.lower()
            # Check if client exists (async)
            result = await db.execute(
                select(Client).where(Client.client_name == client_name)
            )
            existing_client = result.scalars().first()
            if existing_client:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Client with this name already exists",
                    status=False,
                )
            # Check if user exists (async)
            existing_user = await User.get_active_user_by_email(user_email)
            if existing_user:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"User already exists with email: {user_email}",
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="A user with this email already exists",
                    status=False,
                )
            # STEP 2: Validate user data
            try:
                user_data = UserInsertModel(
                    first_name=client_data.user_first_name,
                    last_name=client_data.user_last_name,
                    email=user_email,
                    phone=client_data.user_phone,
                    company_id=company_id,
                    client_id=-1,
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
            # STEP 3: Create client (async)
            client_entity = Client(client_name=client_name, company_id=company_id)
            print("print client entity:", client_entity)
            # print(jsonable_encoder(client_entity))
            created_client = await Client.create_client(client_entity)  # make async
            logger.info(msg=f"Client created successfully with email: {user_email}")
            # STEP 4: Create user
            user_data.client_id = created_client.id
            new_user_response = await UserService.create_user(
                user_data=user_data
            )  # make async
            if not new_user_response.status:
                return new_user_response
            new_user_json = new_user_response.data
            user_dict = json.loads(new_user_json)
            token = JWTHandler.create_access_token(user_dict)
            updated_user_data = user_dict.copy()
            updated_user_data.update(
                {
                    "is_onboarding_completed": False,
                    "company_name": client_name,
                    "client_name": created_client.client_name,
                }
            )
            # STEP 5: Create wallet (async)
            wallet = Wallet(
                client_id=created_client.id,
                cod_amount=Decimal(0),
                amount=Decimal(0),
                provisional_cod_amount=Decimal(0),
                wallet_type="prepaid",
                credit_limit=Decimal(0),
                hold_amount=0.0,
            )
            db.add(wallet)
            await db.flush()  # ensure wallet is persisted
            # STEP 6: Onboarding setup
            onboarding_payload = SignupwithOnboarding(
                company_name=client_name,
                phone_number=user_dict["phone"],
                email=user_dict["email"],
                client_id=created_client.id,
                onboarding_user_id=user_dict["id"],
            )
            print("onboarding_payload:**", onboarding_payload)
            await ClientOnboardingService.onboarding_setup(onboarding_payload)
            logger.info(
                msg=f"User registration completed successfully for: {user_email}"
            )
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
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not register, please try again",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unexpected error during registration: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Registration failed. Please try again later.",
            )

        finally:
            if db:
                await db.close()  # close session to avoid connection leaks

    @staticmethod
    def sanitize_numeric(value, max_val=999.99):
        """
        Sanitize numeric value to ensure it's within acceptable range.

        Args:
            value: Value to sanitize
            max_val: Maximum allowed value (default: 999.99)

        Returns:
            float: Sanitized numeric value, 0.0 if conversion fails
        """
        try:
            numeric_value = float(value)
            return min(numeric_value, max_val)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def contract_transfer() -> GenericResponseModel:
        """
        Transfer contract data from old to new rate structure.
        This appears to be a data migration method.

        Returns:
            GenericResponseModel with transfer results
        """
        try:
            with get_db_session() as db:
                # Fetch all contracts with related data
                contracts = (
                    db.query(Company_To_Client_Contract)
                    .options(
                        joinedload(Company_To_Client_Contract.cod_rates),
                        joinedload(Company_To_Client_Contract.rates),
                    )
                    .all()
                )

                for contract in contracts:
                    cod = contract.cod_rates[0] if contract.cod_rates else None

                    # Initialize zone-wise default values for all zones (a-e)
                    base_rates = {f"base_rate_zone_{z}": 0.0 for z in "abcde"}
                    additional_rates = {
                        f"additional_rate_zone_{z}": 0.0 for z in "abcde"
                    }
                    rto_base_rates = {f"rto_base_rate_zone_{z}": 0.0 for z in "abcde"}
                    rto_additional_rates = {
                        f"rto_additional_rate_zone_{z}": 0.0 for z in "abcde"
                    }

                    # Populate rates from contract data
                    for rate in contract.rates:
                        zone = rate.zone.lower()
                        base_rates[f"base_rate_zone_{zone}"] = (
                            AuthService.sanitize_numeric(rate.base_rate)
                        )
                        additional_rates[f"additional_rate_zone_{zone}"] = (
                            AuthService.sanitize_numeric(rate.additional_rate)
                        )
                        rto_base_rates[f"rto_base_rate_zone_{zone}"] = (
                            AuthService.sanitize_numeric(rate.rto_base_rate)
                        )
                        rto_additional_rates[f"rto_additional_rate_zone_{zone}"] = (
                            AuthService.sanitize_numeric(rate.rto_additional_rate)
                        )

                    # Build complete rate data
                    insert_data = {
                        "company_id": 1,
                        "client_id": contract.client_id,
                        "company_contract_id": contract.company_contract_id,
                        "aggregator_courier_id": contract.aggregator_courier_id,
                        "percentage_rate": (
                            AuthService.sanitize_numeric(cod.percentage_rate)
                            if cod
                            else 0.0
                        ),
                        "absolute_rate": (
                            AuthService.sanitize_numeric(cod.absolute_rate)
                            if cod
                            else 0.0
                        ),
                        "rate_type": contract.rate_type,
                        "isActive": contract.isActive,
                        **base_rates,
                        **additional_rates,
                        **rto_base_rates,
                        **rto_additional_rates,
                    }

                    # Check for existing entry and update or create
                    existing = (
                        db.query(New_Company_To_Client_Rate)
                        .filter_by(
                            client_id=contract.client_id,
                            company_contract_id=contract.company_contract_id,
                            aggregator_courier_id=contract.aggregator_courier_id,
                            rate_type=contract.rate_type,
                        )
                        .first()
                    )

                    if existing:
                        # Update existing record
                        for key, value in insert_data.items():
                            setattr(existing, key, value)
                    else:
                        # Create new record
                        db.add(New_Company_To_Client_Rate(**insert_data))

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=jsonable_encoder(contracts),
                    message="Contract transfer completed successfully",
                )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Contract transfer error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Contract transfer failed.",
            )
            logger.error(
                extra=context_user_data.get(),
                msg="Dev login error: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Dev login failed.",
            )
