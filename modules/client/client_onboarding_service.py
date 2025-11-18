import http
from psycopg2 import DatabaseError
from logger import logger
import os
import boto3
from fastapi import UploadFile, File
from fastapi.encoders import jsonable_encoder
from sqlalchemy import DateTime
from datetime import datetime, timedelta, timezone
import random

import uuid


from context_manager.context import context_user_data, get_db_session

# models
from models import (
    Client_Onboarding_Details,
    Client,
    # Client_Bank_Details,
    Client_Onboarding,
)

# schema
from schema.base import GenericResponseModel

from modules.client.client_schema import (
    SignupwithOnboarding,
    OnBoardingForm,
    OtpVerified,
)


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")  #
BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION_NAME = os.environ.get("REGION_NAME")

# Initialize the S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
)


class ClientOnboardingService:

    @staticmethod
    def onboarding_setup(
        onboarding_data: SignupwithOnboarding,
    ) -> GenericResponseModel:
        try:
            db = get_db_session()

            logger.info(
                msg="Payload with onboarding setup: {}".format(
                    onboarding_data.model_dump()
                ),
            )

            user_id = onboarding_data.onboarding_user_id
            client_id = onboarding_data.client_id

            # Check if onboarding already exists
            existing = (
                db.query(Client_Onboarding_Details)
                .filter_by(onboarding_user_id=user_id)
                .first()
            )

            if existing is None:

                new_client_onboarding = Client_Onboarding_Details(
                    client_id=client_id,
                    onboarding_user_id=user_id,
                    company_name=onboarding_data.company_name,
                    phone_number=onboarding_data.phone_number,
                    email=onboarding_data.email,
                    is_stepper=2,
                )

                db.add(new_client_onboarding)
                db.flush()

                logger.info(
                    msg=f"Created new onboarding setup for client_id: {client_id}",
                )

            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    status=False,
                    message="Onboarding details already exist for this client",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Onboarding setup completed successfully",
            )

        except Exception as e:
            logger.error(
                msg=f"Unexpected error during onboarding setup: {e}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def otp_verified(otpVerified: OtpVerified):
        try:
            with get_db_session() as db:
                # Get current logged-in user
                client_id = context_user_data.get().client_id
                # Get onboarding record
                existing = (
                    db.query(Client_Onboarding_Details)
                    .filter_by(client_id=client_id)
                    .first()
                )

                if not existing:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Onboarding details not found.",
                    )

                # 1. Check OTP expired
                # if (
                #     existing.otp_expires_at
                #     and existing.otp_expires_at < datetime.utcnow()
                # ):
                #     return GenericResponseModel(
                #         status_code=http.HTTPStatus.GONE,
                #         status=False,
                #         message="OTP expired. Please request a new OTP.",
                #     )

                # 2. Check correct OTP
                if str(existing.is_otp) != str(otpVerified.otp):
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        status=False,
                        message="Invalid OTP.",
                    )

                # 3. Mark verified
                existing.is_otp_verified = True
                existing.is_otp = ""
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="OTP verified successfully.",
                )

        except DatabaseError as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Database error during OTP verification: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while verifying OTP.",
            )

        except Exception as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Unexpected error during OTP verification: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def resend_otp():
        try:
            with get_db_session() as db:
                print("Resend Otp")
                """Handle stepper 3: Terms and review"""
                # Get client_id from context
                client_id = context_user_data.get().client_id
                # Get existing onboarding details for this client
                onboarding_details = (
                    db.query(Client_Onboarding_Details)
                    .filter_by(client_id=client_id)
                    .first()
                )
                user_data = context_user_data.get()
                logger.info(
                    extra=user_data.model_dump() if user_data else {},
                    msg="Processing onboarding stepper 4",
                )
                # --- Generate and Save OTP ---
                otp = str(random.randint(100000, 999999))  # Always 6 digits
                onboarding_details.is_otp = otp  # Save OTP in DB
                onboarding_details.otp_expires_at = datetime.now(
                    timezone.utc
                ) + timedelta(minutes=5)
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="OTP verified successfully.",
                )
        except DatabaseError as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Database error during OTP verification: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while verifying OTP.",
            )

        except Exception as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Unexpected error during onboarding creation: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def onboarding_create(
        onboarding_form: OnBoardingForm,
    ) -> GenericResponseModel:
        try:
            print("welcome to  onboarding create**")
            with get_db_session() as db:
                user_data = context_user_data.get()
                logger.info(
                    extra=user_data.model_dump() if user_data else {},
                    msg="Payload onboarding_create: {}".format(str(onboarding_form)),
                )
                print(onboarding_form.stepper, "|*|<stepper>|*|")

                # Get client_id from context
                client_id = context_user_data.get().client_id

                # Get existing onboarding details for this client
                onboarding_details = (
                    db.query(Client_Onboarding_Details)
                    .filter_by(client_id=client_id)
                    .first()
                )

                if not onboarding_details:
                    pass
                    # print(">>>>>", jsonable_encoder(onboarding_form))
                    # Create new onboarding if doesn't exist
                    # onboarding_details = Client_Onboarding_Details(
                    #     client_id=client_id,
                    #     # user_id=context_user_data.get().id,  # Track who initiated
                    #     is_stepper=onboarding_form.stepper,
                    # )
                    # db.add(onboarding_details)
                else:
                    # Check if form access is allowed after final submission
                    if not onboarding_details.is_form_access:
                        user_data = context_user_data.get()
                        logger.warning(
                            extra=user_data.model_dump() if user_data else {},
                            msg=f"Form edit attempted for client_id: {client_id} but form access is disabled",
                        )
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.FORBIDDEN,
                            status=False,
                            message="Form editing is not allowed while verfication is in progress",
                        )

                # Handle different stepper stages
                if onboarding_form.stepper == 2:
                    ClientOnboardingService._handle_stepper_2(
                        onboarding_details, onboarding_form
                    )
                # elif onboarding_form.stepper == 3:
                #     ClientOnboardingService._handle_stepper_3(
                #         onboarding_details, onboarding_form, db
                #     )
                elif onboarding_form.stepper == 3:
                    ClientOnboardingService._handle_stepper_3(onboarding_details)
                elif onboarding_form.stepper == 4:
                    result = ClientOnboardingService._handle_stepper_4(
                        onboarding_details
                    )
                    db.query(Client).filter(Client.id == client_id).update(
                        {"is_onboarding_completed": True},  # or False
                        synchronize_session=False,
                    )

                    if not isinstance(result, int):
                        return result

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.CREATED,
                    status=True,
                    message="Onboarding step completed successfully",
                )

        except DatabaseError as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Database error during onboarding creation: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while processing onboarding.",
            )
        except Exception as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Unexpected error during onboarding creation: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def _handle_stepper_2(onboarding_details, onboarding_form):
        """Handle stepper 2: Company details"""
        user_data = context_user_data.get()
        logger.info(
            extra=user_data.model_dump() if user_data else {},
            msg="Processing onboarding stepper 2",
        )

        onboarding_details.company_legal_name = onboarding_form.company_legal_name
        onboarding_details.company_name = onboarding_form.company_name
        onboarding_details.landmark = onboarding_form.landmark
        onboarding_details.pincode = onboarding_form.pincode
        onboarding_details.city = onboarding_form.city
        onboarding_details.office_address = onboarding_form.office_address
        onboarding_details.state = onboarding_form.state
        onboarding_details.country = onboarding_form.country
        onboarding_details.phone_number = onboarding_form.phone_number
        onboarding_details.email = onboarding_form.email
        onboarding_details.is_company_details = True
        onboarding_details.is_stepper = onboarding_form.stepper

    # @staticmethod
    # def _handle_stepper_3(onboarding_details, onboarding_form, db):
    #     """Handle stepper 3: Billing details and bank information"""
    #     logger.info(
    #         extra=context_user_data.get(),
    #         msg="Processing onboarding stepper 3",
    #     )

    #     # Update billing details
    #     onboarding_details.pan_number = onboarding_form.pan_number
    #     onboarding_details.upload_pan = onboarding_form.upload_pan
    #     onboarding_details.aadhar_card = onboarding_form.aadhar_card
    #     onboarding_details.upload_aadhar_card_front = (
    #         onboarding_form.upload_aadhar_card_front
    #     )
    #     onboarding_details.upload_aadhar_card_back = (
    #         onboarding_form.upload_aadhar_card_back
    #     )
    #     onboarding_details.is_coi = onboarding_form.is_coi
    #     onboarding_details.coi = onboarding_form.coi
    #     onboarding_details.upload_coi = onboarding_form.upload_coi
    #     onboarding_details.is_gst = onboarding_form.is_gst
    #     onboarding_details.gst = onboarding_form.gst
    #     onboarding_details.upload_gst = onboarding_form.upload_gst
    #     onboarding_details.is_stepper = onboarding_form.stepper
    #     onboarding_details.is_billing_details = True

    #     # Handle bank details - use client_id instead of user_id
    #     client_id = context_user_data.get().client_id
    #     existing_bank_details = (
    #         db.query(Client_Bank_Details)
    #         .filter_by(
    #             client_id=client_id,
    #             client_onboarding_id=onboarding_details.id,
    #         )
    #         .first()
    #     )

    #     if existing_bank_details:
    #         logger.info(
    #             extra=context_user_data.get(),
    #             msg="Updating existing bank details",
    #         )
    #         existing_bank_details.beneficiary_name = onboarding_form.beneficiary_name
    #         existing_bank_details.bank_name = onboarding_form.bank_name
    #         existing_bank_details.account_no = onboarding_form.account_no
    #         existing_bank_details.account_type = onboarding_form.account_type
    #         existing_bank_details.ifsc_code = onboarding_form.ifsc_code
    #         existing_bank_details.upload_cheque = onboarding_form.upload_cheque
    #     else:
    #         logger.info(
    #             extra=context_user_data.get(),
    #             msg="Creating new bank details",
    #         )
    #         new_bank_details = Client_Bank_Details(
    #             client_id=client_id,
    #             client_onboarding_id=onboarding_details.id,
    #             beneficiary_name=onboarding_form.beneficiary_name,
    #             bank_name=onboarding_form.bank_name,
    #             account_no=onboarding_form.account_no,
    #             account_type=onboarding_form.account_type,
    #             ifsc_code=onboarding_form.ifsc_code,
    #             upload_cheque=onboarding_form.upload_cheque,
    #             user_id=context_user_data.get().id,
    #         )
    #         db.add(new_bank_details)

    @staticmethod
    def _handle_stepper_3(onboarding_details):
        """Handle stepper 3: Terms and review"""
        user_data = context_user_data.get()
        logger.info(
            extra=user_data.model_dump() if user_data else {},
            msg="Processing onboarding stepper 4",
        )

        onboarding_details.is_term = True
        onboarding_details.is_stepper = 3
        # --- Generate and Save OTP ---
        otp = str(random.randint(100000, 999999))  # Always 6 digits
        onboarding_details.is_otp = otp  # Save OTP in DB
        onboarding_details.otp_expires_at = datetime.now(timezone.utc) + timedelta(
            minutes=5
        )

    @staticmethod
    def _handle_stepper_4(onboarding_details):
        """Handle stepper 4: Final submission"""
        user_data = context_user_data.get()
        logger.info(
            extra=user_data.model_dump() if user_data else {},
            msg="Processing onboarding stepper 4 - Final submission",
        )

        final_result = ClientOnboardingService.final_form_submission(
            onboarding_details.id
        )

        if isinstance(final_result, int):
            onboarding_details.is_review = True
            onboarding_details.is_form_access = False
            onboarding_details.is_stepper = 4

            user_data = context_user_data.get()
            logger.info(
                extra=user_data.model_dump() if user_data else {},
                msg=f"Final onboarding submission completed for client: {onboarding_details.client_id}",
            )
            return final_result
        else:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Final submission failed",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Final submission failed. Please try again.",
            )

    @staticmethod
    def final_form_submission(onboarding_id: int):
        """
        Handles final form submission and creates Client_Onboarding record.
        Uses client_id from context for proper mapping.
        """
        try:
            logger.info(
                extra=context_user_data.get(),
                msg="Processing final form submission for onboarding_id: {}".format(
                    onboarding_id
                ),
            )

            db = get_db_session()
            client_id = context_user_data.get().client_id

            # Check if onboarding record already exists
            existing_onboarding_record = (
                db.query(Client_Onboarding)
                .filter_by(client_onboarding_details_id=onboarding_id)
                .first()
            )

            if existing_onboarding_record:
                # Update existing record
                existing_onboarding_record.remarks = ""
                existing_onboarding_record.action_type = "new"
                db.commit()

                user_data = context_user_data.get()
                logger.info(
                    extra=user_data.model_dump() if user_data else {},
                    msg=f"Updated existing onboarding record for client_id: {client_id}",
                )
                return existing_onboarding_record.client_onboarding_details_id
            else:
                # Create new onboarding record
                new_onboarding_record = Client_Onboarding(
                    client_onboarding_details_id=onboarding_id,
                    remarks="",
                    client_id=client_id,
                    action_type="new",
                    status=False,
                )
                db.add(new_onboarding_record)
                db.commit()

                user_data = context_user_data.get()
                logger.info(
                    extra=user_data.model_dump() if user_data else {},
                    msg=f"Created new onboarding record for client_id: {client_id}",
                )
                return new_onboarding_record.id

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Database error during final form submission: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not complete final submission, please try again.",
            )
        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Unexpected error during final form submission: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def onboarding_previous(
        onboarding_back,
    ) -> GenericResponseModel:
        """
        Handles onboarding previous/back navigation.
        Works with client_id instead of user_id.
        """
        try:
            with get_db_session() as db:
                logger.info(
                    extra=context_user_data.get(),
                    msg="Payload onboarding_previous: {}".format(str(onboarding_back)),
                )

                client_id = context_user_data.get().client_id

                # Get onboarding details for this client
                onboarding_details = (
                    db.query(Client_Onboarding_Details)
                    .filter_by(client_id=client_id)
                    .first()
                )

                if not onboarding_details:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Onboarding details not found for this client",
                    )

                # Check if form access is allowed after final submission
                if not onboarding_details.is_form_access:
                    logger.warning(
                        extra=context_user_data.get(),
                        msg=f"Form navigation attempted for client_id: {client_id} but form access is disabled",
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.FORBIDDEN,
                        status=False,
                        message="Form editing is not allowed after final submission. Please contact support.",
                    )

                # Handle different back actions
                if onboarding_back.action == "edit" and onboarding_back.stepper == 2:
                    onboarding_details.is_company_details = False
                    onboarding_details.is_billing_details = False
                    onboarding_details.is_term = False

                if onboarding_back.action == "back" and onboarding_back.stepper == 3:
                    onboarding_details.is_term = False

                # if onboarding_back.action == "back" and onboarding_back.stepper == 3:
                #     onboarding_details.is_billing_details = False

                if onboarding_back.action == "back" and onboarding_back.stepper == 2:
                    onboarding_details.is_company_details = False

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Successfully updated",
                )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Database error during onboarding previous: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while processing the request.",
            )
        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Unexpected error during onboarding previous: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_onboarding(stepper: str) -> GenericResponseModel:
        """
        Retrieves onboarding data for a specific stepper.
        Works with client_id instead of user_id.
        """
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                logger.info(
                    extra=user_data.model_dump() if user_data else {},
                    msg="Retrieving onboarding data for stepper: {}".format(stepper),
                )

                client_id = context_user_data.get().client_id

                # Get onboarding details for this client
                existing_onboarding = (
                    db.query(Client_Onboarding_Details)
                    .filter_by(client_id=client_id)
                    .first()
                )

                if not existing_onboarding:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="No onboarding data found for this client",
                        data={},
                    )
                # Base data
                data = {
                    "is_company_details": existing_onboarding.is_company_details,
                    "is_review": existing_onboarding.is_review,
                    "is_form_access": existing_onboarding.is_form_access,
                }
                # Stepper-specific data
                if stepper == "2":
                    data["companyDetails"] = {
                        "company_legal_name": existing_onboarding.company_legal_name,
                        "company_name": existing_onboarding.company_name,
                        "office_address": existing_onboarding.office_address,
                        "landmark": existing_onboarding.landmark,
                        "pincode": existing_onboarding.pincode,
                        "city": existing_onboarding.city,
                        "state": existing_onboarding.state,
                        "country": existing_onboarding.country,
                        "phone_number": existing_onboarding.phone_number,
                        "email": existing_onboarding.email,
                    }

                elif stepper == "3":
                    data["terms"] = {"is_terms": existing_onboarding.is_term}

                elif stepper == "4":
                    # Get client onboarding status
                    client_status = (
                        db.query(
                            Client_Onboarding.status,
                            Client_Onboarding.action_type,
                            Client_Onboarding.remarks,
                        )
                        .filter_by(client_onboarding_details_id=existing_onboarding.id)
                        .first()
                    )
                    # Status information
                    data["status"] = client_status.status if client_status else None
                    data["remarks"] = client_status.remarks if client_status else None
                    data["action_type"] = (
                        client_status.action_type if client_status else None
                    )

                    # Company details
                    data["companyDetails"] = {
                        "company_legal_name": existing_onboarding.company_legal_name,
                        "company_name": existing_onboarding.company_name,
                        "office_address": existing_onboarding.office_address,
                        "landmark": existing_onboarding.landmark,
                        "pincode": existing_onboarding.pincode,
                        "city": existing_onboarding.city,
                        "state": existing_onboarding.state,
                        "country": existing_onboarding.country,
                        "phone_number": existing_onboarding.phone_number,
                        "is_otp_verified": existing_onboarding.is_otp_verified,
                        "otp_expires_at": existing_onboarding.otp_expires_at,
                        "email": existing_onboarding.email,
                    }
                    # data["bankingDetails"] = None

                    # Terms
                    data["terms"] = {"is_terms": existing_onboarding.is_term}

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Data retrieved successfully",
                    data=data,
                )

        except DatabaseError as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Database error during get_onboarding: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while retrieving onboarding data.",
            )
        except Exception as e:
            user_data = context_user_data.get()
            logger.error(
                extra=user_data.model_dump() if user_data else {},
                msg="Unexpected error during get_onboarding: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    async def onboarding_doc_upload(
        name: str, file: UploadFile
    ) -> GenericResponseModel:
        try:
            logger.info(
                extra=context_user_data.get(),
                msg=f"Initiating onboarding_doc_upload for file: {file.filename}, name: {name}",
            )

            client_id = context_user_data.get().client_id
            logger.info(
                extra=context_user_data.get(),
                msg=f"Retrieved client_id: {client_id}",
            )

            # Check if form access is allowed after final submission
            with get_db_session() as db:
                onboarding_details = (
                    db.query(Client_Onboarding_Details)
                    .filter_by(client_id=client_id)
                    .first()
                )

                if onboarding_details and not onboarding_details.is_form_access:
                    logger.warning(
                        extra=context_user_data.get(),
                        msg=f"Document upload attempted for client_id: {client_id} but form access is disabled",
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.FORBIDDEN,
                        status=False,
                        message="Document upload is not allowed after final submission. Please contact support.",
                    )

            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            logger.info(
                extra=context_user_data.get(),
                msg=f"Resolved BASE_DIR: {BASE_DIR}",
            )

            UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
            logger.info(
                extra=context_user_data.get(),
                msg=f"Computed UPLOAD_DIR: {UPLOAD_DIR}",
            )

            file_extension = file.filename.split(".")[-1].lower()
            logger.info(
                extra=context_user_data.get(),
                msg=f"Extracted file extension: {file_extension}",
            )

            allowed_extensions = {"jpg", "jpeg", "png", "pdf"}
            if file_extension not in allowed_extensions:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Invalid file type: {file_extension}. Allowed: {allowed_extensions}",
                )
                raise ValueError("Invalid file type. Allowed: jpg, jpeg, png, pdf")

            unique_filename = f"{name}_{uuid.uuid4()}.{file_extension}"
            logger.info(
                extra=context_user_data.get(),
                msg=f"Generated unique_filename: {unique_filename}",
            )

            s3_key = f"{client_id}/Client_Onboarding/{unique_filename}"
            logger.info(
                extra=context_user_data.get(),
                msg=f"Computed S3 key: {s3_key}",
            )

            logger.info(
                extra=context_user_data.get(),
                msg=f"Uploading file to S3 bucket: {BUCKET_NAME}, region: {REGION_NAME}",
            )

            s3_client.upload_fileobj(
                file.file,
                BUCKET_NAME,
                s3_key,
                ExtraArgs={"ContentType": file.content_type},
            )

            logger.info(
                extra=context_user_data.get(),
                msg=f"File uploaded successfully to S3. Key: {s3_key}",
            )

            file_url = f"https://{BUCKET_NAME}.s3.{REGION_NAME}.amazonaws.com/{s3_key}"
            logger.info(
                extra=context_user_data.get(),
                msg=f"Generated S3 file URL: {file_url}",
            )

            return GenericResponseModel(
                status_code=201,
                status=True,
                message="File uploaded successfully",
                data={"file_name": file.filename, "path": file_url},
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Exception in onboarding_doc_upload: {str(e)}",
            )
            return GenericResponseModel(
                status_code=500,
                status=False,
                message=f"File upload failed: {str(e)}",
            )
