"""
Audit Logger
Comprehensive logging for security events and user actions
"""

from datetime import datetime
from typing import Optional, Dict, Any
from logger import logger
from database.db import UTC


class AuditLogger:
    """Centralized audit logging for security events"""

    # Event types
    EVENT_USER_LOGIN_SUCCESS = "user_login_success"
    EVENT_USER_LOGIN_FAILED = "user_login_failed"
    EVENT_USER_SIGNUP = "user_signup"
    EVENT_OTP_SENT = "otp_sent"
    EVENT_OTP_VERIFIED = "otp_verified"
    EVENT_OTP_VERIFICATION_FAILED = "otp_verification_failed"
    EVENT_OTP_MAX_ATTEMPTS_EXCEEDED = "otp_max_attempts_exceeded"
    EVENT_OTP_RATE_LIMIT_EXCEEDED = "otp_rate_limit_exceeded"
    EVENT_ACCOUNT_LOCKED = "account_locked"
    EVENT_PHONE_NUMBER_UPDATED = "phone_number_updated"
    EVENT_ACCOUNT_DEACTIVATED = "account_deactivated"
    EVENT_SUSPICIOUS_ACTIVITY = "suspicious_activity_detected"
    EVENT_CAPTCHA_FAILED = "captcha_verification_failed"
    EVENT_UNAUTHORIZED_ACCESS_ATTEMPT = "unauthorized_access_attempt"
    EVENT_TOKEN_EXPIRED = "token_expired"
    EVENT_INVALID_TOKEN = "invalid_token"

    # Event categories
    CATEGORY_AUTHENTICATION = "authentication"
    CATEGORY_AUTHORIZATION = "authorization"
    CATEGORY_SECURITY = "security"
    CATEGORY_USER_ACTION = "user_action"

    @staticmethod
    def log_event(
        event_type: str,
        user_id: Optional[int] = None,
        user_email: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        severity: str = "info",
        category: str = "security",
    ):
        """
        Log a security/audit event with comprehensive details.
        Saves to both application logs and database.
        Automatically extracts IP address and user agent from request context if not provided.

        Args:
            event_type: Type of event (use EVENT_* constants)
            user_id: User ID if applicable
            user_email: User email if applicable
            ip_address: IP address of the request (auto-extracted if not provided)
            user_agent: User agent string (auto-extracted if not provided)
            details: Additional event-specific details
            severity: Log severity (info, warning, error, critical)
            category: Event category (authentication, authorization, security, user_action)
        """
        # Auto-extract request info if not provided
        endpoint = None
        if ip_address is None or user_agent is None or endpoint is None:
            try:
                from utils.request_helper import RequestHelper

                request_info = RequestHelper.get_request_info()
                ip_address = ip_address or request_info.get("ip_address", "unknown")
                user_agent = user_agent or request_info.get("user_agent", "unknown")
                endpoint = request_info.get("endpoint", "unknown")
            except Exception as e:
                # Gracefully handle if request context not available
                ip_address = ip_address or "unknown"
                user_agent = user_agent or "unknown"
                endpoint = "unknown"

        log_data = {
            "event_type": event_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "user_id": user_id,
            "user_email": user_email,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "endpoint": endpoint,
            "details": details or {},
        }

        # Create log message
        message = f"[AUDIT] {event_type}"
        if user_email:
            message += f" | User: {user_email}"
        if user_id:
            message += f" | ID: {user_id}"
        if ip_address and ip_address != "unknown":
            message += f" | IP: {ip_address}"

        # Log based on severity
        if severity == "critical":
            logger.critical(extra=log_data, msg=message)
        elif severity == "error":
            logger.error(extra=log_data, msg=message)
        elif severity == "warning":
            logger.warning(extra=log_data, msg=message)
        else:
            logger.info(extra=log_data, msg=message)

        # Save to database
        try:
            from models.audit_log import AuditLog

            audit_data = {
                "event_type": event_type,
                "event_category": category,
                "severity": severity,
                "user_id": user_id,
                "user_email": user_email,
                "message": message,
                "context": details,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "endpoint": endpoint,
            }

            AuditLog.create_log(audit_data)
        except Exception as e:
            # Don't fail the main operation if audit logging fails
            logger.error(f"Failed to save audit log to database: {str(e)}")

    @staticmethod
    def log_login_success(
        user_id: int,
        user_email: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        is_otp_verified: bool = True,
    ):
        """Log successful login"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_USER_LOGIN_SUCCESS,
            user_id=user_id,
            user_email=user_email,
            ip_address=ip_address,
            user_agent=user_agent,
            details={"is_otp_verified": is_otp_verified},
            severity="info",
            category=AuditLogger.CATEGORY_AUTHENTICATION,
        )

    @staticmethod
    def log_login_failed(
        user_email: str,
        reason: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """Log failed login attempt"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_USER_LOGIN_FAILED,
            user_email=user_email,
            ip_address=ip_address,
            user_agent=user_agent,
            details={"reason": reason},
            severity="warning",
            category=AuditLogger.CATEGORY_AUTHENTICATION,
        )

    @staticmethod
    def log_signup(
        user_id: int,
        user_email: str,
        phone_number: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """Log user signup"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_USER_SIGNUP,
            user_id=user_id,
            user_email=user_email,
            ip_address=ip_address,
            user_agent=user_agent,
            details={"phone_number": phone_number},
            severity="info",
            category=AuditLogger.CATEGORY_AUTHENTICATION,
        )

    @staticmethod
    def log_otp_sent(
        user_id: int, user_email: str, phone_number: str, otp_type: str = "phone"
    ):
        """Log OTP sent"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_OTP_SENT,
            user_id=user_id,
            user_email=user_email,
            details={"phone_number": phone_number, "otp_type": otp_type},
            severity="info",
            category=AuditLogger.CATEGORY_AUTHENTICATION,
        )

    @staticmethod
    def log_otp_verified(user_id: int, user_email: str, attempts: int = 1):
        """Log successful OTP verification"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_OTP_VERIFIED,
            user_id=user_id,
            user_email=user_email,
            details={"attempts": attempts},
            severity="info",
            category=AuditLogger.CATEGORY_AUTHENTICATION,
        )

    @staticmethod
    def log_otp_verification_failed(
        user_id: int,
        user_email: str,
        attempts: int,
        max_attempts: int,
        ip_address: Optional[str] = None,
    ):
        """Log failed OTP verification attempt"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_OTP_VERIFICATION_FAILED,
            user_id=user_id,
            user_email=user_email,
            ip_address=ip_address,
            details={
                "attempts": attempts,
                "max_attempts": max_attempts,
                "remaining_attempts": max_attempts - attempts,
            },
            severity="warning",
            category=AuditLogger.CATEGORY_SECURITY,
        )

    @staticmethod
    def log_otp_max_attempts_exceeded(
        user_id: int, user_email: str, ip_address: Optional[str] = None
    ):
        """Log OTP max attempts exceeded"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_OTP_MAX_ATTEMPTS_EXCEEDED,
            user_id=user_id,
            user_email=user_email,
            ip_address=ip_address,
            details={},
            severity="error",
            category=AuditLogger.CATEGORY_SECURITY,
        )

    @staticmethod
    def log_otp_rate_limit_exceeded(
        user_id: int, user_email: str, otp_count: int, ip_address: Optional[str] = None
    ):
        """Log OTP rate limit exceeded"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_OTP_RATE_LIMIT_EXCEEDED,
            user_id=user_id,
            user_email=user_email,
            ip_address=ip_address,
            details={"otp_count": otp_count},
            severity="warning",
            category=AuditLogger.CATEGORY_SECURITY,
        )

    @staticmethod
    def log_phone_number_updated(
        user_id: int, user_email: str, old_phone: str, new_phone: str
    ):
        """Log phone number update"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_PHONE_NUMBER_UPDATED,
            user_id=user_id,
            user_email=user_email,
            details={"old_phone": old_phone, "new_phone": new_phone},
            severity="info",
            category=AuditLogger.CATEGORY_USER_ACTION,
        )

    @staticmethod
    def log_captcha_failed(
        user_email: Optional[str] = None,
        action: str = "",
        score: Optional[float] = None,
        ip_address: Optional[str] = None,
    ):
        """Log CAPTCHA verification failure"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_CAPTCHA_FAILED,
            user_email=user_email,
            ip_address=ip_address,
            details={"action": action, "score": score},
            severity="warning",
            category=AuditLogger.CATEGORY_SECURITY,
        )

    @staticmethod
    def log_unauthorized_access(
        user_id: Optional[int] = None,
        user_email: Optional[str] = None,
        reason: str = "",
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """Log unauthorized access attempt"""
        AuditLogger.log_event(
            event_type=AuditLogger.EVENT_UNAUTHORIZED_ACCESS_ATTEMPT,
            user_id=user_id,
            user_email=user_email,
            ip_address=ip_address,
            user_agent=user_agent,
            details={"reason": reason},
            severity="error",
            category=AuditLogger.CATEGORY_AUTHORIZATION,
        )
