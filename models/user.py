from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, Index, func, text
from sqlalchemy.orm import Session, relationship
from sqlalchemy.dialects.postgresql import JSON

from database import DBBaseClass, DBBase


class User(DBBase, DBBaseClass):
    __tablename__ = "user"

    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(255), nullable=False)
    password_hash = Column(String(255), nullable=False)
    status = Column(
        String(20), nullable=False, default="active"
    )  # user can be inactive or active

    company_id = Column(Integer, ForeignKey("company.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    company = relationship("Company", back_populates="users")
    client = relationship("Client", back_populates="users")

    extra_credentials = Column(String, nullable=True)
    is_otp_verified = Column(Boolean, nullable=False, default=False)

    # Indexes for authentication and OTP flow optimization
    __table_args__ = (
        # Unique constraint for email (case-insensitive) - prevents duplicate accounts
        Index(
            'idx_user_email_unique',
            text('LOWER(email)'),
            unique=True,
            postgresql_where=text('is_deleted = false')
        ),
        # Unique constraint for phone - prevents duplicate phone numbers
        Index(
            'idx_user_phone_unique',
            'phone',
            unique=True,
            postgresql_where=text('is_deleted = false')
        ),
        # Index for fast user lookup by email during login
        Index(
            'idx_user_email',
            'email',
            postgresql_where=text("is_deleted = false AND status = 'active'")
        ),
        # Index for duplicate phone number checks
        Index(
            'idx_user_phone',
            'phone',
            postgresql_where=text('is_deleted = false')
        ),
        # Composite index for faster user lookups by email and status
        Index('idx_user_email_status', 'email', 'status', 'is_deleted'),
        # Index for OTP verification status checks
        Index(
            'idx_user_otp_verified',
            'is_otp_verified',
            postgresql_where=text('is_deleted = false')
        ),
    )

    def __to_model(self):
        from modules.user.user_schema import UserModel

        return UserModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self, password_hash: str):
        entity = self.model_dump()
        entity["password_hash"] = password_hash
        entity.pop("password")
        return User(**entity)

    @classmethod
    def create_user(cls, user):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        db.add(user)
        db.flush()

        return user.__to_model()

    @classmethod
    def get_by_id(cls, id):
        user = super().get_by_id(id)
        return user.__to_model() if user else None

    @classmethod
    def get_by_uuid(cls, uuid):
        user = super().get_by_uuid(uuid)
        return user.__to_model() if user else None

    @classmethod
    def get_active_user_by_email(cls, email, eager_load_relations=False):
        from context_manager.context import get_db_session

        db = get_db_session()
        query = db.query(cls)

        # Optionally eager load company and client relationships to avoid N+1 queries
        if eager_load_relations:
            from sqlalchemy.orm import joinedload
            query = query.options(
                joinedload(cls.company),
                joinedload(cls.client)
            )

        user = query.filter(
            cls.status == "active",
            cls.email == email,
            cls.is_deleted.is_(False),
        ).first()

        return user.__to_model() if user else None

    @classmethod
    def get_active_user_by_email_with_relations(cls, email):
        """
        Get active user by email with company and client data eager loaded.
        Returns tuple: (user_model, company_name, client_name) or (None, None, None)
        This avoids N+1 queries by using a single query with joinedload.
        """
        from context_manager.context import get_db_session
        from sqlalchemy.orm import joinedload

        db = get_db_session()
        user_entity = (
            db.query(cls)
            .options(
                joinedload(cls.company),
                joinedload(cls.client)
            )
            .filter(
                cls.status == "active",
                cls.email == email,
                cls.is_deleted.is_(False),
            )
            .first()
        )

        if not user_entity:
            return None, None, None

        # Access relationship data before converting to model
        company_name = user_entity.company.company_name if user_entity.company else None
        client_name = user_entity.client.client_name if user_entity.client else None

        return user_entity.__to_model(), company_name, client_name

    @classmethod
    def update_user_by_uuid(cls, user_uuid: str, update_dict: dict) -> int:
        from context_manager.context import get_db_session

        db = get_db_session()
        update_query = db.query(cls).filter(
            cls.uuid == user_uuid, cls.is_deleted.is_(False)
        )

        updates = update_query.update(update_dict)
        db.flush()
        return updates
