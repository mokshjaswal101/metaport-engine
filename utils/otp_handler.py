"""
Generic OTP Handler
Provides complete OTP functionality for any use case (registration, password reset, 2FA, etc.)
Includes both low-level utilities and high-level business logic.
"""

import http
import random
import secrets
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from psycopg2 import DatabaseError

from logger import logger
from database.db import UTC
from context_manager.context import get_db_session

# models
from models.otp_verification import OTP_Verification
from models.user import User

# schema
from schema.base import GenericResponseModel

# utils
from utils.audit_logger import AuditLogger


class OTPHandler:
    """
    Complete OTP Handler for various use cases.

    Use cases:
    - User registration verification
    - Login verification (2FA)
    - Password reset
    - Email verification
    - Phone number change
    - Sensitive operations confirmation
    """

    # OTP Configuration
    OTP_LENGTH = 6
    OTP_EXPIRY_MINUTES = 10
    OTP_TYPE_PHONE = "phone"
    OTP_TYPE_EMAIL = "email"

    # Rate Limiting & Security
    MAX_OTP_ATTEMPTS = 5
    MAX_OTP_REQUESTS_PER_WINDOW = 3
    RATE_LIMIT_WINDOW_MINUTES = 10
    RESEND_COOLDOWN_SECONDS = 60

    # ==================== PRIVATE UTILITY METHODS ====================

    @staticmethod
    def _generate_otp() -> str:
        """
        Generate a 6-digit OTP code.

        Returns:
            str: 6-digit OTP code
        """
        otp = secrets.randbelow(900000) + 100000
        return str(otp)

    @staticmethod
    def _get_otp_expiry_time() -> datetime:
        """
        Calculate OTP expiry time (current time + configured minutes).

        Returns:
            datetime: Expiry timestamp in UTC
        """
        expiry = datetime.now(UTC) + timedelta(minutes=OTPHandler.OTP_EXPIRY_MINUTES)
        return expiry

    @staticmethod
    def _is_otp_expired(expires_at: datetime) -> bool:
        """
        Check if OTP has expired.

        Args:
            expires_at: OTP expiry timestamp

        Returns:
            bool: True if expired, False otherwise
        """
        current_time = datetime.now(UTC)

        # Ensure expires_at is timezone-aware for comparison
        if expires_at.tzinfo is None:
            # If naive, assume it's UTC
            expires_at = expires_at.replace(tzinfo=UTC)

        is_expired = current_time > expires_at

        if is_expired:
            logger.info(f"OTP expired. Expiry: {expires_at}, Current: {current_time}")

        return is_expired

    @staticmethod
    def _validate_otp_code(input_otp: str, stored_otp: str) -> bool:
        """
        Validate OTP code by comparing input with stored value.

        Args:
            input_otp: OTP code entered by user
            stored_otp: OTP code stored in database

        Returns:
            bool: True if match, False otherwise
        """
        is_valid = input_otp.strip() == stored_otp.strip()

        if is_valid:
            logger.info("OTP validation successful")
        else:
            logger.warning("OTP validation failed - code mismatch")

        return is_valid

    @staticmethod
    def _send_otp_via_phone(phone_number: str, otp_code: str) -> Dict[str, Any]:

        try:
            # Import WhatsApp service
            from modules.whatsapp.whatsapp_service import WhatsappService

            # Send OTP via WhatsApp
            result = WhatsappService.send_otp_via_whatsapp(
                phone_number=phone_number, otp_code=otp_code
            )

            if result.get("status"):
                logger.info(
                    f"✅ OTP sent successfully via WhatsApp to +91{phone_number}"
                )
            else:
                logger.error(
                    f"❌ Failed to send OTP via WhatsApp: {result.get('message')}"
                )

            return result

        except ImportError as e:
            logger.error(f"Failed to import WhatsApp service: {str(e)}")
            # Fallback to mock/log mode
            logger.info("=" * 50)
            logger.info("📱 SENDING OTP VIA SMS (FALLBACK - SERVICE UNAVAILABLE)")
            logger.info(f"Phone Number: +91{phone_number}")
            logger.info(
                f"Message: Your Metaport verification code is {otp_code}. Valid for {OTPHandler.OTP_EXPIRY_MINUTES} minutes."
            )
            logger.info("=" * 50)

            return {
                "status": True,
                "message": "OTP sent successfully (fallback mode)",
                "phone_number": phone_number,
                "otp_code": otp_code,  # Remove in production
            }

        except Exception as e:
            logger.error(f"Error sending OTP via WhatsApp: {str(e)}")
            # Fallback to mock/log mode
            logger.info("=" * 50)
            logger.info("📱 SENDING OTP VIA SMS (FALLBACK - ERROR)")
            logger.info(f"Phone Number: +91{phone_number}")
            logger.info(
                f"Message: Your Metaport verification code is {otp_code}. Valid for {OTPHandler.OTP_EXPIRY_MINUTES} minutes."
            )
            logger.info("=" * 50)

            return {
                "status": True,
                "message": "OTP sent successfully (fallback mode)",
                "phone_number": phone_number,
                "otp_code": otp_code,  # Remove in production
            }

    @staticmethod
    def _send_otp_via_email(email: str, otp_code: str) -> Dict[str, Any]:
        """
        Send OTP via Email (Mock implementation - replace with actual email service).

        Args:
            email: Recipient email address
            otp_code: OTP code to send

        Returns:
            dict: Status response
        """
        # TODO: Integrate with Email service (SendGrid, AWS SES, etc.)
        logger.info("=" * 50)
        logger.info("📧 SENDING OTP VIA EMAIL (MOCK)")
        logger.info(f"Email: {email}")
        logger.info(f"Subject: Metaport - Your Verification Code")
        logger.info(
            f"Body: Your verification code is {otp_code}. Valid for {OTPHandler.OTP_EXPIRY_MINUTES} minutes."
        )
        logger.info("=" * 50)

        return {
            "status": True,
            "message": "OTP sent successfully (mock)",
            "email": email,
            "otp_code": otp_code,  # Remove in production
        }

    @staticmethod
    def _send_otp(
        user_data: Dict[str, Any], otp_code: str, otp_type: str
    ) -> Dict[str, Any]:

        try:
            if otp_type == OTPHandler.OTP_TYPE_PHONE:
                phone = user_data.get("phone") or user_data.get("phone_number")
                if not phone:
                    return {
                        "status": False,
                        "message": "Phone number not found in user data",
                    }
                return OTPHandler._send_otp_via_phone(phone, otp_code)

            elif otp_type == OTPHandler.OTP_TYPE_EMAIL:
                email = user_data.get("email")
                if not email:
                    return {"status": False, "message": "Email not found in user data"}
                return OTPHandler._send_otp_via_email(email, otp_code)

            else:
                return {"status": False, "message": f"Invalid OTP type: {otp_type}"}

        except Exception as e:
            logger.error(f"Error sending OTP: {str(e)}")
            return {"status": False, "message": f"Failed to send OTP: {str(e)}"}

    @staticmethod
    def _create_otp_data(user_id: int, otp_type: str) -> Dict[str, Any]:
        """
        Generate OTP data for database storage.

        Args:
            user_id: User ID
            otp_type: Type of OTP (phone/email)

        Returns:
            dict: OTP data including code and expiry
        """
        otp_code = OTPHandler._generate_otp()
        expires_at = OTPHandler._get_otp_expiry_time()

        return {
            "user_id": user_id,
            "otp_code": otp_code,
            "otp_type": otp_type,
            "expires_at": expires_at,
        }

    # ==================== PUBLIC BUSINESS LOGIC METHODS ====================

    @staticmethod
    def generate_and_send_otp(
        user_id: int, otp_type: str = OTP_TYPE_PHONE, purpose: str = "verification"
    ) -> GenericResponseModel:

        try:

            # Get user data
            user = User.get_by_id(user_id)
            if not user:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    status=False,
                    message="User not found.",
                )

            # Check cooldown period (60 seconds between requests)
            last_otp_time = OTP_Verification.get_last_otp_sent_time(user_id)
            if last_otp_time:
                current_time = datetime.now(UTC)
                time_diff = current_time - last_otp_time
                seconds_elapsed = int(time_diff.total_seconds())
                seconds_remaining = max(
                    0, OTPHandler.RESEND_COOLDOWN_SECONDS - seconds_elapsed
                )

                if seconds_remaining > 0:
                    logger.warning(
                        msg=f"Cooldown active for user_id: {user_id}, {seconds_remaining}s remaining"
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.TOO_MANY_REQUESTS,
                        status=False,
                        message=f"Please wait {seconds_remaining} seconds before requesting a new OTP.",
                        data={
                            "cooldown_active": True,
                            "seconds_remaining": seconds_remaining,
                            "cooldown_seconds": OTPHandler.RESEND_COOLDOWN_SECONDS,
                        },
                    )

            # Check rate limiting (3 requests per 10 minutes)
            recent_otp_count = OTP_Verification.get_recent_otp_count(
                user_id, minutes=OTPHandler.RATE_LIMIT_WINDOW_MINUTES
            )

            if recent_otp_count >= OTPHandler.MAX_OTP_REQUESTS_PER_WINDOW:
                logger.warning(
                    msg=f"Rate limit exceeded for user_id: {user_id}, count: {recent_otp_count}"
                )
                AuditLogger.log_otp_rate_limit_exceeded(
                    user_id=user_id, user_email=user.email, otp_count=recent_otp_count
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.TOO_MANY_REQUESTS,
                    status=False,
                    message=f"Too many OTP requests. Please wait {OTPHandler.RATE_LIMIT_WINDOW_MINUTES} minutes before trying again.",
                    data={
                        "rate_limit_exceeded": True,
                        "requests_made": recent_otp_count,
                        "max_requests": OTPHandler.MAX_OTP_REQUESTS_PER_WINDOW,
                        "window_minutes": OTPHandler.RATE_LIMIT_WINDOW_MINUTES,
                    },
                )

            # Invalidate all previous OTPs for this user
            OTP_Verification.invalidate_all_user_otps(user_id)

            # Generate new OTP
            otp_data = OTPHandler._create_otp_data(user_id=user_id, otp_type=otp_type)

            # Save OTP to database
            OTP_Verification.create_otp(otp_data)

            # Prepare user data for sending
            user_dict = {
                "id": user.id,
                "phone": user.phone,
                "email": user.email,
                "first_name": user.first_name,
                "last_name": user.last_name,
            }

            # Send OTP
            send_result = OTPHandler._send_otp(
                user_data=user_dict, otp_code=otp_data["otp_code"], otp_type=otp_type
            )

            print(send_result)

            if not send_result.get("status"):
                logger.error(msg=f"Failed to send OTP to user_id: {user_id}")
                error_message = send_result.get("message", "Unknown error")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Failed to send OTP. Please check your phone number and try again.",
                    data={"error": error_message},
                )

            # Audit log
            AuditLogger.log_otp_sent(
                user_id=user_id,
                user_email=user.email,
                phone_number=user.phone,
                otp_type=otp_type,
            )

            logger.info(
                msg=f"OTP sent successfully for user_id: {user_id}, purpose: {purpose}"
            )

            # Create user-friendly message based on OTP type
            if otp_type == OTPHandler.OTP_TYPE_PHONE:
                phone_display = (
                    f"****{user.phone[-4:]}" if len(user.phone) >= 4 else user.phone
                )
                message = f"OTP sent to your phone number ending in {phone_display}. Valid for {OTPHandler.OTP_EXPIRY_MINUTES} minutes."
            else:
                message = f"OTP sent to your email. Valid for {OTPHandler.OTP_EXPIRY_MINUTES} minutes."

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message=message,
                data={
                    "otp_type": otp_type,
                    "expires_in_minutes": OTPHandler.OTP_EXPIRY_MINUTES,
                    "cooldown_seconds": OTPHandler.RESEND_COOLDOWN_SECONDS,
                    "max_attempts": OTPHandler.MAX_OTP_ATTEMPTS,
                },
            )

        except DatabaseError as e:
            logger.error(
                msg=f"Database error during OTP generation: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An error occurred while sending OTP.",
            )

        except Exception as e:
            logger.error(
                msg=f"Unexpected error during OTP generation: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def verify_otp(
        user_id: int,
        otp_code: str,
        mark_user_verified: bool = False,
        purpose: str = "verification",
    ) -> GenericResponseModel:

        try:
            logger.info(
                msg=f"OTP verification attempt for user_id: {user_id}, purpose: {purpose}"
            )

            db = get_db_session()
            if not db:
                logger.error(msg="Database session is None")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Database connection error.",
                )

            input_otp = otp_code.strip()
            # Security: Don't log actual OTP code
            logger.info(msg=f"OTP verification attempt for user_id: {user_id}")

            # First, get the latest OTP (regardless of expiry or used status) to provide specific feedback
            logger.info(msg=f"Fetching latest OTP for user_id: {user_id}")
            latest_otp = OTP_Verification.get_latest_otp_by_user_id(user_id)

            if not latest_otp:
                logger.warning(msg=f"No OTP record found for user_id: {user_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    status=False,
                    message="No OTP found. Please request an OTP first.",
                )

            # Check if OTP has been used
            if latest_otp.is_used:
                logger.warning(msg=f"OTP already used for user_id: {user_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.GONE,
                    status=False,
                    message="This OTP has already been used. Please request a new OTP.",
                )

            # Check if OTP has expired
            if OTPHandler._is_otp_expired(latest_otp.expires_at):
                logger.warning(msg=f"OTP expired for user_id: {user_id}")

                # Calculate how long ago it expired for better UX
                from datetime import datetime
                from database.db import UTC

                current_time = datetime.now(UTC)
                expires_at = latest_otp.expires_at
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=UTC)

                time_expired = current_time - expires_at
                minutes_expired = int(time_expired.total_seconds() / 60)

                if minutes_expired < 5:
                    message = "Your OTP has expired. Please request a new one."
                else:
                    message = f"Your OTP expired {minutes_expired} minutes ago. Please request a new one."

                return GenericResponseModel(
                    status_code=http.HTTPStatus.GONE,
                    status=False,
                    message=message,
                    data={"expired": True, "minutes_ago": minutes_expired},
                )

            # Use the latest OTP for validation
            otp_record = latest_otp

            # Check if maximum attempts exceeded
            if otp_record.attempts >= OTPHandler.MAX_OTP_ATTEMPTS:
                logger.warning(
                    msg=f"Maximum OTP attempts exceeded for user_id: {user_id}"
                )
                user = User.get_by_id(user_id)
                if user:
                    AuditLogger.log_otp_max_attempts_exceeded(
                        user_id=user_id, user_email=user.email
                    )
                OTP_Verification.mark_otp_as_used(otp_record.id)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.TOO_MANY_REQUESTS,
                    status=False,
                    message=f"Too many incorrect attempts ({OTPHandler.MAX_OTP_ATTEMPTS}). Please request a new OTP.",
                    data={
                        "max_attempts_exceeded": True,
                        "attempts": otp_record.attempts,
                    },
                )

            # Increment attempts
            current_attempts = OTP_Verification.increment_attempts(otp_record.id)

            # Validate OTP code
            if not OTPHandler._validate_otp_code(input_otp, otp_record.otp_code):
                remaining_attempts = OTPHandler.MAX_OTP_ATTEMPTS - current_attempts
                logger.warning(
                    msg=f"Invalid OTP for user_id: {user_id}, attempt {current_attempts}/{OTPHandler.MAX_OTP_ATTEMPTS}, {remaining_attempts} remaining"
                )
                user = User.get_by_id(user_id)
                if user:
                    AuditLogger.log_otp_verification_failed(
                        user_id=user_id,
                        user_email=user.email,
                        attempts=current_attempts,
                        max_attempts=OTPHandler.MAX_OTP_ATTEMPTS,
                    )

                # Provide clear, actionable message
                if remaining_attempts == 1:
                    message = "Incorrect OTP. You have 1 attempt remaining before this OTP expires."
                elif remaining_attempts > 1:
                    message = f"Incorrect OTP. You have {remaining_attempts} attempts remaining."
                else:
                    message = "Incorrect OTP. No attempts remaining. Please request a new OTP."

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message=message,
                    data={
                        "attempts_used": current_attempts,
                        "attempts_remaining": remaining_attempts,
                        "max_attempts": OTPHandler.MAX_OTP_ATTEMPTS,
                    },
                )

            # OTP is valid - mark as used
            OTP_Verification.mark_otp_as_used(otp_record.id)

            # Get user for audit log and optional verification
            user = User.get_by_id(user_id)
            if not user:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    status=False,
                    message="User not found.",
                )

            # Mark user as OTP verified if requested (for registration flow)
            # Check if user is already verified to prevent concurrent verification
            if mark_user_verified:
                # Reload user from database to get latest state (prevent race condition)
                try:
                    if db and hasattr(db, 'refresh'):
                        db.refresh(user)
                except Exception:
                    # If refresh fails, reload user from database
                    user = User.get_by_id(user_id)
                    if not user:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.NOT_FOUND,
                            status=False,
                            message="User not found.",
                        )
                
                # Check if user is already verified (concurrent verification protection)
                if user.is_otp_verified:
                    logger.warning(
                        msg=f"User {user_id} is already OTP verified. Concurrent verification attempt prevented."
                    )
                    # Still return success since OTP was valid, but don't update again
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        status=True,
                        message="Phone number already verified!",
                        data={
                            "verified": True,
                            "attempts": current_attempts,
                            "user_verified": True,
                            "already_verified": True,
                        },
                    )
                
                # Update user verification status atomically
                User.update_user_by_uuid(
                    user_uuid=user.uuid, update_dict={"is_otp_verified": True}
                )
                
                # Verify the update was successful
                updated_user = User.get_by_id(user_id)
                if updated_user and not updated_user.is_otp_verified:
                    logger.error(
                        msg=f"Failed to mark user {user_id} as verified after OTP verification"
                    )

            # Audit log successful verification
            AuditLogger.log_otp_verified(
                user_id=user_id, user_email=user.email, attempts=current_attempts
            )

            logger.info(
                msg=f"OTP verified successfully for user_id: {user_id}, purpose: {purpose}, attempts: {current_attempts}"
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Phone number verified successfully! You can now access your account.",
                data={
                    "verified": True,
                    "attempts": current_attempts,
                    "user_verified": mark_user_verified,
                },
            )

        except RuntimeError as e:
            logger.error(
                msg=f"Runtime error during OTP verification: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Database session error. Please try again.",
            )

        except DatabaseError as e:
            logger.error(
                msg=f"Database error during OTP verification: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An error occurred while verifying OTP.",
            )

        except AttributeError as e:
            logger.error(
                msg=f"Attribute error during OTP verification: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal error occurred. Please try again later.",
            )

        except Exception as e:
            logger.error(
                msg=f"Unexpected error during OTP verification: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_otp_status(user_id: int) -> GenericResponseModel:
        """
        Get OTP status for user including time since last OTP sent.

        Args:
            user_id: User ID

        Returns:
            GenericResponseModel with OTP status data
        """
        try:
            # Get last OTP sent time
            last_otp_time = OTP_Verification.get_last_otp_sent_time(user_id)

            if not last_otp_time:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "can_resend": True,
                        "seconds_remaining": 0,
                        "last_sent_at": None,
                    },
                    message="No OTP sent yet",
                )

            # Calculate seconds since last OTP
            current_time = datetime.now(UTC)
            time_diff = current_time - last_otp_time
            seconds_elapsed = int(time_diff.total_seconds())

            # Resend cooldown
            seconds_remaining = max(
                0, OTPHandler.RESEND_COOLDOWN_SECONDS - seconds_elapsed
            )
            can_resend = seconds_remaining == 0

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "can_resend": can_resend,
                    "seconds_remaining": seconds_remaining,
                    "last_sent_at": last_otp_time.isoformat(),
                    "cooldown_seconds": OTPHandler.RESEND_COOLDOWN_SECONDS,
                },
                message="OTP status retrieved successfully",
            )

        except Exception as e:
            logger.error(msg=f"Error getting OTP status: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Failed to get OTP status",
            )

    @staticmethod
    def update_phone_and_resend_otp(
        user_id: int, new_phone: str, allow_verified_users: bool = False
    ) -> GenericResponseModel:
        """
        Update phone number and send new OTP.

        Args:
            user_id: User ID
            new_phone: New phone number (10 digits)
            allow_verified_users: Whether to allow phone update for verified users

        Returns:
            GenericResponseModel with update status
        """
        try:
            db = get_db_session()

            # Validate phone number format
            if not new_phone or len(new_phone) != 10 or not new_phone.isdigit():
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Invalid phone number. Please enter a valid 10-digit number.",
                )

            logger.info(msg=f"Phone number update request for user_id: {user_id}")

            # Get user data
            user = User.get_by_id(user_id)
            if not user:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    status=False,
                    message="User not found.",
                )

            # Check if phone update is allowed based on verification status
            if user.is_otp_verified and not allow_verified_users:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.FORBIDDEN,
                    status=False,
                    message="Phone number cannot be changed after verification.",
                )

            # Check if phone number is already in use by another user
            from sqlalchemy import and_

            existing_user = (
                db.query(User)
                .filter(
                    and_(
                        User.phone == new_phone,
                        User.id != user_id,
                        User.is_deleted.is_(False),
                    )
                )
                .first()
            )

            if existing_user:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    status=False,
                    message="This phone number is already registered.",
                )

            # Store old phone for audit log
            old_phone = user.phone

            # Update phone number
            User.update_user_by_uuid(
                user_uuid=user.uuid, update_dict={"phone": new_phone}
            )

            logger.info(msg=f"Phone number updated for user_id: {user_id}")

            # Audit log phone number update
            AuditLogger.log_phone_number_updated(
                user_id=user_id,
                user_email=user.email,
                old_phone=old_phone,
                new_phone=new_phone,
            )

            # Generate and send new OTP
            otp_result = OTPHandler.generate_and_send_otp(
                user_id=user_id,
                otp_type=OTPHandler.OTP_TYPE_PHONE,
                purpose="phone_update",
            )

            if not otp_result.status:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="Phone number updated but failed to send OTP. Please try resending.",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Phone number updated successfully! New OTP sent.",
                data={"new_phone": new_phone, "otp_sent": True},
            )

        except DatabaseError as e:
            logger.error(
                msg=f"Database error during phone update: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An error occurred while updating phone number.",
            )

        except Exception as e:
            logger.error(
                msg=f"Unexpected error during phone update: {str(e)}",
                exc_info=True
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )
