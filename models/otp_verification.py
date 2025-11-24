from sqlalchemy import Column, String, Boolean, Integer, ForeignKey, TIMESTAMP, Index, text
from sqlalchemy.orm import Session

from database.db import DBBaseClass, DBBase, time_now


class OTP_Verification(DBBase, DBBaseClass):
    __tablename__ = "otp_verifications"

    user_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    otp_code = Column(String(6), nullable=False)
    otp_type = Column(String(20), default="phone")  # 'phone' or 'email' for future use
    expires_at = Column(TIMESTAMP(timezone=True), nullable=False)
    verified_at = Column(TIMESTAMP(timezone=True), nullable=True)
    attempts = Column(Integer, default=0)
    is_used = Column(Boolean, default=False)

    # Indexes for OTP verification and cleanup optimization
    __table_args__ = (
        # Index for OTP lookup by user_id (most frequent query)
        Index(
            'idx_otp_user_id',
            'user_id',
            'is_deleted',
            postgresql_where=text('is_deleted = false')
        ),
        # Index for OTP cleanup jobs and expiry checks
        Index(
            'idx_otp_expires_at',
            'expires_at',
            postgresql_where=text('is_deleted = false')
        ),
        # Composite index for getting valid OTP records
        Index(
            'idx_otp_user_valid',
            'user_id',
            'is_used',
            'expires_at',
            text('created_at DESC'),
            postgresql_where=text('is_deleted = false')
        ),
        # Index for rate limiting queries (count recent OTPs)
        Index(
            'idx_otp_created_at',
            'user_id',
            text('created_at DESC'),
            postgresql_where=text('is_deleted = false')
        ),
    )

    def __to_model(self):
        from schema.otp_schema import OTPVerificationModel

        # All datetime fields are now timezone-aware at the database level
        # No need for manual timezone conversion
        return OTPVerificationModel.model_validate(self)

    @classmethod
    def create_otp(cls, otp_data):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        otp_verification = cls(**otp_data)
        db.add(otp_verification)
        db.flush()

        return otp_verification.__to_model()

    @classmethod
    def get_latest_otp_by_user_id(cls, user_id: int):
        """Get the latest OTP for a user (used and unused)"""
        from context_manager.context import get_db_session

        db = get_db_session()
        otp = (
            db.query(cls)
            .filter(
                cls.user_id == user_id,
                cls.is_deleted.is_(False),
            )
            .order_by(cls.created_at.desc())
            .first()
        )

        return otp.__to_model() if otp else None

    @classmethod
    def get_valid_otp_by_user_id(cls, user_id: int):
        """Get the latest valid (unused and not expired) OTP for a user"""
        from context_manager.context import get_db_session
        from datetime import datetime
        from database.db import UTC

        db = get_db_session()
        current_time = datetime.now(UTC)

        otp = (
            db.query(cls)
            .filter(
                cls.user_id == user_id,
                cls.is_used.is_(False),
                cls.expires_at > current_time,
                cls.is_deleted.is_(False),
            )
            .order_by(cls.created_at.desc())
            .first()
        )

        return otp.__to_model() if otp else None

    @classmethod
    def mark_otp_as_used(cls, otp_id: int) -> int:
        """Mark an OTP as used"""
        from context_manager.context import get_db_session
        from datetime import datetime
        from database.db import UTC

        db = get_db_session()
        update_query = db.query(cls).filter(
            cls.id == otp_id,
            cls.is_deleted.is_(False)
        )

        updates = update_query.update({
            "is_used": True,
            "verified_at": datetime.now(UTC)
        })
        db.flush()
        return updates

    @classmethod
    def invalidate_all_user_otps(cls, user_id: int) -> int:
        """Mark all user's OTPs as used (when generating new OTP)"""
        from context_manager.context import get_db_session

        db = get_db_session()
        update_query = db.query(cls).filter(
            cls.user_id == user_id,
            cls.is_used.is_(False),
            cls.is_deleted.is_(False)
        )

        updates = update_query.update({"is_used": True})
        db.flush()
        return updates

    @classmethod
    def increment_attempts(cls, otp_id: int) -> int:
        """Increment the attempts counter for an OTP"""
        from context_manager.context import get_db_session

        db = get_db_session()
        otp = db.query(cls).filter(
            cls.id == otp_id,
            cls.is_deleted.is_(False)
        ).first()

        if otp:
            otp.attempts += 1
            db.flush()
            return otp.attempts
        return 0

    @classmethod
    def get_recent_otp_count(cls, user_id: int, minutes: int = 10) -> int:
        """
        Count OTPs generated in last N minutes for rate limiting.
        
        Args:
            user_id: User ID
            minutes: Time window in minutes (default: 10)
            
        Returns:
            Count of OTPs generated in the time window
        """
        from context_manager.context import get_db_session
        from datetime import datetime, timedelta
        from database.db import UTC
        from sqlalchemy import func

        db = get_db_session()
        cutoff_time = datetime.now(UTC) - timedelta(minutes=minutes)

        count = db.query(func.count(cls.id)).filter(
            cls.user_id == user_id,
            cls.created_at >= cutoff_time,
            cls.is_deleted.is_(False)
        ).scalar()

        return count or 0

    @classmethod
    def get_last_otp_sent_time(cls, user_id: int):
        """
        Get the timestamp of the last OTP sent to user.
        Used for frontend timer validation.
        
        Args:
            user_id: User ID
            
        Returns:
            datetime or None: Timestamp of last OTP sent (timezone-aware)
        """
        from context_manager.context import get_db_session
        from database.db import UTC

        db = get_db_session()
        
        latest_otp = (
            db.query(cls)
            .filter(
                cls.user_id == user_id,
                cls.is_deleted.is_(False)
            )
            .order_by(cls.created_at.desc())
            .first()
        )

        if not latest_otp:
            return None

        # Datetime is already timezone-aware from database
        return latest_otp.created_at
